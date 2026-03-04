from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
import pendulum
import io
import oracledb
import pyarrow.parquet as pq
import gc
import numpy as np

class S3ToOracleOperator(BaseOperator):
    """
    [Smart Loader Operator]
    S3 -> Oracle 적재
    - Full Load: {prefix}/{prefix}_full.{ext} 최신 파일 1개만 적재 (혹은 {prefix}.{ext})
    - Incremental: {prefix}/{YYYY}/{YYYYMM}/{prefix}_{YYYYMM}.{ext} 적재
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'target_table', 'key_prefix', 'date_column')

    def __init__(
        self,
        oracle_conn_id,
        minio_conn_id,
        target_table,
        bucket_name,
        from_date=None,
        to_date=None,
        key_prefix='taxi',
        date_column=None,
        file_extension='parquet',
        csv_delimiter=',',
        csv_has_header=True,
        batch_size=100000,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.minio_conn_id = minio_conn_id
        self.target_table = target_table
        self.bucket_name = bucket_name
        self.from_date = from_date
        self.to_date = to_date
        self.key_prefix = key_prefix
        self.date_column = date_column
        self.file_extension = file_extension
        self.csv_delimiter = csv_delimiter
        self.csv_has_header = csv_has_header
        self.batch_size = batch_size

    def _get_oracle_conn(self):
        oracle_hook = OracleHook(oracle_conn_id=self.oracle_conn_id)
        conn_info = oracle_hook.get_connection(self.oracle_conn_id)
        service_name = conn_info.schema if conn_info.schema else 'Oracle23ai'
        dsn = f"{conn_info.host}:{conn_info.port}/{service_name}"
        
        conn = oracledb.connect(
            user=conn_info.login,
            password=conn_info.password,
            dsn=dsn
        )
        return conn

    def _preprocess_data(self, df):
        """데이터 타입별 NULL 처리 및 형변환"""
        
        num_cols = df.select_dtypes(include=['number']).columns
        obj_cols = df.select_dtypes(include=['object', 'string']).columns

        date_keywords = ['DATE', 'TIME', 'SINCE', 'DT', 'TIMESTAMP', 'DAY']
        for col in df.columns:
            if any(k in col.upper() for k in date_keywords):
                if col in num_cols or col in obj_cols:
                    continue 
                df[col] = pd.to_datetime(df[col], errors='coerce')

        if len(num_cols) > 0:
            df[num_cols] = df[num_cols].fillna(0)
        
        if len(obj_cols) > 0:
            for col in obj_cols:
                # pandas 에서는 '\\N' 이나 빈 문자열보다 NaN 상태에서 None 으로 바꿔주는 것이 Oracle에 적합
                df[col] = df[col].astype(str).replace({'nan': None, 'None': None, '<NA>': None})
                df[col] = df[col].str.strip()
                df[col] = df[col].replace({'': None})
        
        df = df.replace({np.nan: None})
        
        return df

    def execute(self, context):
        conn = self._get_oracle_conn()
        cursor = conn.cursor()
        s3_hook = S3Hook(aws_conn_id=self.minio_conn_id)

        try:
            def is_valid_date(d):
                return d and str(d).strip().lower() not in ['none', '', 'null']

            has_date = is_valid_date(self.from_date) and is_valid_date(self.to_date)

            if not has_date:
                self.log.info(f"📦 [Full Load] 날짜 범위 없음 -> 테이블({self.target_table}) TRUNCATE 실행")
                cursor.execute(f"TRUNCATE TABLE {self.target_table}")
                conn.commit()
                self.log.info(f"✅ TRUNCATE 완료: {self.target_table}")

                # 우선 확장자가 이미 key_prefix에 포함된 경우인지 확인
                if self.key_prefix.endswith(f".{self.file_extension}"):
                    file_key = self.key_prefix
                else:
                    filename = f"{self.key_prefix}_full.{self.file_extension}"
                    file_key = f"{self.key_prefix}/{filename}"
                    
                    if not s3_hook.check_for_key(file_key, bucket_name=self.bucket_name):
                        # 폴백: prefix.ext 가 존재하는지 확인 (예: kkbox-churn-prediction-challenge/train.csv)
                        fallback_key = f"{self.key_prefix}.{self.file_extension}"
                        self.log.info(f"⚠️ {file_key} 가 없어 {fallback_key} 를 탐색합니다.")
                        if s3_hook.check_for_key(fallback_key, bucket_name=self.bucket_name):
                            file_key = fallback_key
                
                self.log.info(f"📂 파일 탐색: {file_key}")
                
                if s3_hook.check_for_key(file_key, bucket_name=self.bucket_name):
                    self._load_single_file(s3_hook, cursor, file_key, conn)
                else:
                    self.log.warning(f"⚠️ Full Load 파일이 없습니다: {file_key}")

            else:
                self.log.info(f"🔄 [Incremental Load] 기간: {self.from_date} ~ {self.to_date}")
                try:
                    start_dt = pendulum.from_format(str(self.from_date), 'YYYYMMDD')
                    end_dt = pendulum.from_format(str(self.to_date), 'YYYYMMDD')
                except ValueError:
                    start_dt = pendulum.parse(str(self.from_date))
                    end_dt = pendulum.parse(str(self.to_date))

                current_dt = start_dt
                while current_dt <= end_dt:
                    year = current_dt.format('YYYY')
                    month = current_dt.format('MM')
                    yyyymm = current_dt.format('YYYYMM')
                    
                    if self.date_column and str(self.date_column).lower() != 'none':
                        next_month = current_dt.add(months=1).format('YYYY-MM-01')
                        current_month_start = current_dt.format('YYYY-MM-01')
                        
                        delete_sql = f"""
                            DELETE FROM {self.target_table} 
                            WHERE {self.date_column} >= TO_DATE('{current_month_start}', 'YYYY-MM-DD') 
                              AND {self.date_column} < TO_DATE('{next_month}', 'YYYY-MM-DD')
                        """
                        self.log.info(f"🧹 기간 삭제 실행 ({year}-{month})")
                        cursor.execute(delete_sql)

                    filename = f"{self.key_prefix}_{yyyymm}.{self.file_extension}"
                    file_key = f"{self.key_prefix}/{year}/{yyyymm}/{filename}"
                    
                    if s3_hook.check_for_key(file_key, bucket_name=self.bucket_name):
                        self._load_single_file(s3_hook, cursor, file_key, conn)
                    else:
                        self.log.warning(f"⚠️ 파일 없음 (Skip): {file_key}")

                    current_dt = current_dt.add(months=1)

        except Exception as e:
            conn.rollback()
            self.log.error(f"❌ 에러 발생: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()

    def _load_single_file(self, s3_hook, cursor, file_key, conn):
        self.log.info(f"📥 적재 시작: {file_key}")
        
        file_obj = s3_hook.get_key(key=file_key, bucket_name=self.bucket_name)
        data_stream = io.BytesIO(file_obj.get()['Body'].read())
        
        total_rows = 0
        if self.file_extension.lower() == 'parquet':
            parquet_file = pq.ParquetFile(data_stream)
            target_columns = parquet_file.schema.names
            
            insert_sql = f"""
            INSERT INTO {self.target_table} ({', '.join(target_columns)}) 
            VALUES ({', '.join([':' + str(i+1) for i in range(len(target_columns))])})
            """

            for batch in parquet_file.iter_batches(batch_size=self.batch_size):
                df_chunk = batch.to_pandas()
                df_chunk = self._preprocess_data(df_chunk)
                
                rows = [tuple(x) for x in df_chunk.to_numpy()]
                cursor.executemany(insert_sql, rows)
                
                total_rows += len(rows)
                del df_chunk, rows
                gc.collect()
                
        elif self.file_extension.lower() == 'csv':
            header_param = 'infer' if self.csv_has_header else None
            for df_chunk in pd.read_csv(data_stream, sep=self.csv_delimiter, header=header_param, chunksize=self.batch_size):
                
                if not self.csv_has_header:
                    target_columns = [f"col_{i}" for i in range(len(df_chunk.columns))]
                    df_chunk.columns = target_columns
                else:
                    target_columns = df_chunk.columns.tolist()
                    
                df_chunk = self._preprocess_data(df_chunk)
                
                insert_sql = f"""
                INSERT INTO {self.target_table} ({', '.join(target_columns)}) 
                VALUES ({', '.join([':' + str(i+1) for i in range(len(target_columns))])})
                """
                
                rows = [tuple(x) for x in df_chunk.to_numpy()]
                cursor.executemany(insert_sql, rows)
                
                total_rows += len(rows)
                del df_chunk, rows
                gc.collect()
        else:
            raise ValueError(f"지원하지 않는 확장자입니다: {self.file_extension}")
            
        conn.commit()
        self.log.info(f"✅ 적재 완료: {total_rows}건")