from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import pendulum
import io
import pyarrow.parquet as pq
import gc

class S3ParquetToPostgresOperator(BaseOperator):
    """
    [Smart Loader Operator]
    S3 -> Postgres 적재
    - Full Load: {prefix}/{prefix}_full.{ext} 최신 파일 1개만 적재
    - Incremental: {prefix}/{YYYY}/{YYYYMM}/{prefix}_{YYYYMM}.{ext} 적재
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'target_table', 'key_prefix', 'date_column')

    def __init__(
        self,
        postgres_conn_id,
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
        self.postgres_conn_id = postgres_conn_id
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

    def _get_postgres_conn(self):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        return pg_hook.get_conn()

    def _preprocess_data(self, df):
        """데이터 타입별 NULL 처리 및 형변환"""
        
        # 컬럼들의 실제 데이터 타입(Type) 분류
        num_cols = df.select_dtypes(include=['number']).columns
        obj_cols = df.select_dtypes(include=['object', 'string']).columns

        # 1. 날짜 컬럼 강제 변환
        date_keywords = ['DATE', 'TIME', 'SINCE', 'DT', 'TIMESTAMP', 'DAY']
        for col in df.columns:
            if any(k in col.upper() for k in date_keywords):
                # ▼▼▼ [핵심 수정] 숫자형(int, float 등)이거나 문자열(object)이면 날짜 변환에서 무조건 제외 ▼▼▼
                if col in num_cols or col in obj_cols:
                    continue 
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # 2. 숫자형 컬럼만 NULL -> 0 변환
        if len(num_cols) > 0:
            df[num_cols] = df[num_cols].fillna(0)
        
        # 3. 문자열 컬럼 처리 (NULL -> \N)
        if len(obj_cols) > 0:
            for col in obj_cols:
                df[col] = df[col].fillna('\\N').astype(str).str.strip()
        
        return df

    def execute(self, context):
        conn = self._get_postgres_conn()
        cursor = conn.cursor()
        s3_hook = S3Hook(aws_conn_id=self.minio_conn_id)

        try:
            def is_valid_date(d):
                return d and str(d).strip().lower() not in ['none', '', 'null']

            has_date = is_valid_date(self.from_date) and is_valid_date(self.to_date)

            # =========================================================
            # CASE 1: Full Load (테이블명_full.parquet 찾기)
            # =========================================================
            if not has_date:
                self.log.info(f"📦 [Full Load] 날짜 범위 없음 -> 테이블({self.target_table}) TRUNCATE 실행")
                cursor.execute(f"TRUNCATE TABLE {self.target_table} RESTART IDENTITY CASCADE")
                conn.commit()
                self.log.info(f"✅ TRUNCATE 완료: {self.target_table}")

                filename = f"{self.key_prefix}_full.{self.file_extension}"
                file_key = f"{self.key_prefix}/{filename}"
                
                self.log.info(f"📂 파일 탐색: {file_key}")
                
                if s3_hook.check_for_key(file_key, bucket_name=self.bucket_name):
                    self._load_single_file(s3_hook, cursor, file_key, conn)
                else:
                    self.log.warning(f"⚠️ Full Load 파일이 없습니다: {file_key}")

            # =========================================================
            # CASE 2: Incremental Load (YYYY/YYYYMM/테이블명_YYYYMM.parquet 찾기)
            # =========================================================
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
                            WHERE {self.date_column} >= '{current_month_start}' 
                              AND {self.date_column} < '{next_month}'
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
            
            for batch in parquet_file.iter_batches(batch_size=self.batch_size):
                df_chunk = batch.to_pandas()
                df_chunk = self._preprocess_data(df_chunk)
                
                csv_buffer = io.StringIO()
                df_chunk.to_csv(csv_buffer, index=False, header=False, sep='\t', na_rep='\\N')
                csv_buffer.seek(0)
                
                cursor.copy_expert(
                    f"COPY {self.target_table} ({', '.join(target_columns)}) FROM STDIN", 
                    csv_buffer
                )
                total_rows += len(df_chunk)
                del df_chunk, csv_buffer
                gc.collect()
        elif self.file_extension.lower() == 'csv':
            header_param = 'infer' if self.csv_has_header else None
            for df_chunk in pd.read_csv(data_stream, sep=self.csv_delimiter, header=header_param, chunksize=self.batch_size):
                
                # 헤더가 없는 경우 임의의 컬럼명이 생성되므로 Postgres 테이블 컬럼 순서대로 들어간다고 가정
                if not self.csv_has_header:
                    # Postgres 테이블의 실제 컬럼들을 조회해서 매핑할 수도 있으나,
                    # 성능 및 복잡성을 위해 단순히 순환하며 DataFrame의 컬럼 개수만큼 처리하도록 함
                    target_columns = [f"col_{i}" for i in range(len(df_chunk.columns))]
                    df_chunk.columns = target_columns
                else:
                    target_columns = df_chunk.columns.tolist()
                    
                df_chunk = self._preprocess_data(df_chunk)
                
                csv_buffer = io.StringIO()
                df_chunk.to_csv(csv_buffer, index=False, header=False, sep='\t', na_rep='\\N')
                csv_buffer.seek(0)
                
                # 헤더가 없으면 COPY 시에 대상 컬럼을 명시하지 않거나, 생성된 컬럼으로 복사됨
                # 대상 테이블 스키마와 1:1 매핑된다고 가정
                if not self.csv_has_header:
                     cursor.copy_expert(
                        f"COPY {self.target_table} FROM STDIN", 
                        csv_buffer
                    )
                else:
                    cursor.copy_expert(
                        f"COPY {self.target_table} ({', '.join(target_columns)}) FROM STDIN", 
                        csv_buffer
                    )
                total_rows += len(df_chunk)
                del df_chunk, csv_buffer
                gc.collect()
        else:
            raise ValueError(f"지원하지 않는 확장자입니다: {self.file_extension}")
            
        conn.commit()
        self.log.info(f"✅ 적재 완료: {total_rows}건")