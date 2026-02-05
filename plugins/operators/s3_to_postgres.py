from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
# ▼ [추가] 고속 적재를 위한 라이브러리
from psycopg2.extras import execute_values 
import pandas as pd
import pendulum
import io
import pyarrow.parquet as pq
import gc

class S3ParquetToPostgresOperator(BaseOperator):
    """
    [Custom Operator]
    S3(MinIO) -> PostgreSQL 고속 적재 (Batch Insert 적용)
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'target_table')

    def __init__(
        self,
        postgres_conn_id,
        minio_conn_id,
        target_table,
        bucket_name,
        from_date,
        to_date,
        key_prefix='taxi',
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
        self.batch_size = batch_size

    def _get_postgres_conn(self):
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        return pg_hook.get_conn()

    def _preprocess_data(self, df):
        str_cols = ['STORE_AND_FWD_FLAG', 'VENDOR_ID', 'RATE_CODE_ID', 
                    'PAYMENT_TYPE', 'PULOCATION_ID', 'DOLOCATION_ID']
        
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].fillna('N').astype(str).str.strip()

        df = df.fillna(0)
        
        date_cols = ['TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df

    def execute(self, context):
        self.log.info(f"🚀 [S3ParquetToPostgresOperator] 고속 적재 시작: {self.from_date} ~ {self.to_date}")
        
        conn = self._get_postgres_conn()
        cursor = conn.cursor()

        try:
            try:
                start_dt = pendulum.from_format(str(self.from_date), 'YYYYMMDD')
                end_dt = pendulum.from_format(str(self.to_date), 'YYYYMMDD')
            except ValueError:
                start_dt = pendulum.parse(str(self.from_date))
                end_dt = pendulum.parse(str(self.to_date))

            current_dt = start_dt
            s3_hook = S3Hook(aws_conn_id=self.minio_conn_id)

            while current_dt <= end_dt:
                year = current_dt.format('YYYY')
                month = current_dt.format('MM')
                
                file_key = f"{self.key_prefix}/year={year}/month={month}/yellow_tripdata_{year}-{month}.parquet"
                self.log.info(f"📂 파일 탐색: {file_key}")
                
                file_obj = s3_hook.get_key(key=file_key, bucket_name=self.bucket_name)
                
                if not file_obj:
                    self.log.warning(f"⚠️ 파일 없음 (Skip): {file_key}")
                    current_dt = current_dt.add(months=1)
                    continue

                data_stream = io.BytesIO(file_obj.get()['Body'].read())
                parquet_file = pq.ParquetFile(data_stream)

                target_columns = [
                    'VENDOR_ID', 'TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME', 
                    'PASSENGER_COUNT', 'TRIP_DISTANCE', 'RATE_CODE_ID', 
                    'STORE_AND_FWD_FLAG', 'PULOCATION_ID', 'DOLOCATION_ID', 
                    'PAYMENT_TYPE', 'FARE_AMOUNT', 'EXTRA', 'MTA_TAX', 
                    'TIP_AMOUNT', 'TOLLS_AMOUNT', 'IMPROVEMENT_SURCHARGE', 
                    'TOTAL_AMOUNT', 'CONGESTION_SURCHARGE', 'AIRPORT_FEE'
                ]
                
                # [수정] execute_values를 위한 SQL 문법
                # "INSERT INTO table (cols) VALUES %s" <- %s 자리에 데이터 뭉치가 들어감
                insert_sql = f"INSERT INTO {self.target_table} ({', '.join(target_columns)}) VALUES %s"

                total_rows = 0
                for batch in parquet_file.iter_batches(batch_size=self.batch_size):
                    df_chunk = batch.to_pandas()
                    
                    df_chunk = df_chunk.rename(columns={
                        'VendorID': 'VENDOR_ID', 'tpep_pickup_datetime': 'TPEP_PICKUP_DATETIME',
                        'tpep_dropoff_datetime': 'TPEP_DROPOFF_DATETIME', 'passenger_count': 'PASSENGER_COUNT',
                        'trip_distance': 'TRIP_DISTANCE', 'RatecodeID': 'RATE_CODE_ID',
                        'store_and_fwd_flag': 'STORE_AND_FWD_FLAG', 'PULocationID': 'PULOCATION_ID',
                        'DOLocationID': 'DOLOCATION_ID', 'payment_type': 'PAYMENT_TYPE',
                        'fare_amount': 'FARE_AMOUNT', 'extra': 'EXTRA', 'mta_tax': 'MTA_TAX',
                        'tip_amount': 'TIP_AMOUNT', 'tolls_amount': 'TOLLS_AMOUNT',
                        'improvement_surcharge': 'IMPROVEMENT_SURCHARGE', 'total_amount': 'TOTAL_AMOUNT',
                        'congestion_surcharge': 'CONGESTION_SURCHARGE', 'airport_fee': 'AIRPORT_FEE'
                    })
                    
                    for col in target_columns:
                        if col not in df_chunk.columns: df_chunk[col] = None
                    
                    df_chunk = self._preprocess_data(df_chunk)
                    df_chunk = df_chunk[target_columns]
                    
                    rows = [tuple(x) for x in df_chunk.to_numpy()]
                    
                    # ▼▼▼ [핵심 수정] executemany 대신 execute_values 사용 ▼▼▼
                    # page_size: 한 번에 DB로 보내는 행의 개수 (성능에 중요)
                    execute_values(cursor, insert_sql, rows, page_size=10000)
                    
                    total_rows += len(rows)
                    
                    del df_chunk, rows
                    gc.collect()

                conn.commit()
                self.log.info(f"✅ {year}-{month} 고속 처리 완료: {total_rows}건 적재됨")
                
                current_dt = current_dt.add(months=1)

        except Exception as e:
            conn.rollback()
            self.log.error(f"❌ 에러 발생: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()