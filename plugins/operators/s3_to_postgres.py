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
    - 날짜 컬럼 자동 감지 강화 (DATE, TIME, SINCE, DT 등)
    - NULL 처리 개선: 숫자는 0, 문자는 'N', 날짜는 NULL 유지
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
        self.batch_size = batch_size

    def _get_postgres_conn(self):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        return pg_hook.get_conn()

    def _preprocess_data(self, df):
        """데이터 타입별 NULL 처리 및 형변환 (매우 중요!)"""
        
        # 1. 날짜 컬럼 강제 변환 (NULL 유지를 위해 가장 먼저 수행)
        # 컬럼명에 아래 키워드가 있으면 날짜로 인식 (euro_in_use_since 대응을 위해 'SINCE' 추가)
        date_keywords = ['DATE', 'TIME', 'SINCE', 'DT', 'TIMESTAMP', 'DAY']
        
        for col in df.columns:
            if any(k in col.upper() for k in date_keywords):
                # errors='coerce'는 변환 실패(NULL 포함) 시 NaT(Not a Time)로 설정 -> DB에는 NULL로 들어감
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # 2. 숫자형 컬럼만 NULL -> 0 변환
        # (날짜 컬럼은 이미 datetime 타입이 되었으므로 여기서 제외됨)
        num_cols = df.select_dtypes(include=['number']).columns
        df[num_cols] = df[num_cols].fillna(0)
        
        # 3. 문자열 컬럼 처리 (NULL -> 빈 문자열 또는 'N')
        # object 타입 중 datetime이 아닌 것들
        obj_cols = df.select_dtypes(include=['object']).columns
        for col in obj_cols:
            df[col] = df[col].fillna('\\N').astype(str).str.strip()
            # 주의: Postgres COPY에서 \N은 NULL을 의미함. 
            # 빈값으로 넣고 싶으면 '' 로 설정. 여기서는 원본 데이터 보존을 위해 \N(NULL) 사용 권장.
            # 만약 'N' 문자로 채우고 싶다면 fillna('N') 사용.
        
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
            # CASE 1: Full Load
            # =========================================================
            if not has_date:
                self.log.info(f"📦 [Full Load] 날짜 범위 없음 -> 테이블({self.target_table}) TRUNCATE 실행")
                cursor.execute(f"TRUNCATE TABLE {self.target_table}")
                conn.commit()

                self.log.info(f"📂 S3 전체 스캔 중: {self.key_prefix}/")
                all_objs = s3_hook.list_keys(bucket_name=self.bucket_name, prefix=self.key_prefix)
                target_files = [f for f in all_objs if f.endswith('.parquet')] if all_objs else []
                
                if not target_files:
                    self.log.warning("⚠️ 적재할 S3 파일이 하나도 없습니다.")
                    return

                self.log.info(f"총 {len(target_files)}개의 파일을 발견했습니다. 순차 적재 시작.")
                for file_key in target_files:
                    self._load_single_file(s3_hook, cursor, file_key, conn)

            # =========================================================
            # CASE 2: Incremental Load
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

                    filename = f"yellow_tripdata_{year}-{month}.parquet"
                    file_key = f"{self.key_prefix}/year={year}/month={month}/{filename}"
                    
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
        parquet_file = pq.ParquetFile(data_stream)
        
        target_columns = parquet_file.schema.names
        
        total_rows = 0
        for batch in parquet_file.iter_batches(batch_size=self.batch_size):
            df_chunk = batch.to_pandas()
            
            # [수정된 전처리 로직 사용]
            df_chunk = self._preprocess_data(df_chunk)
            
            csv_buffer = io.StringIO()
            # na_rep='\\N' -> Pandas의 NaT/NaN/None을 Postgres의 NULL(\N)로 변환
            df_chunk.to_csv(csv_buffer, index=False, header=False, sep='\t', na_rep='\\N')
            csv_buffer.seek(0)
            
            cursor.copy_expert(
                f"COPY {self.target_table} ({', '.join(target_columns)}) FROM STDIN", 
                csv_buffer
            )
            total_rows += len(df_chunk)
            del df_chunk, csv_buffer
            gc.collect()
            
        conn.commit()
        self.log.info(f"✅ 적재 완료: {total_rows}건")