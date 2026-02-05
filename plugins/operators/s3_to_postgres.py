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
    [Custom Operator]
    S3(MinIO) -> PostgreSQL 적재
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'target_table')

    def __init__(
        self,
        postgres_conn_id, # 변경됨
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
        # PostgresHook을 사용하여 연결 객체 가져오기
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        return pg_hook.get_conn()

    def _preprocess_data(self, df):
        # Postgres도 타입 에러 방지를 위해 전처리 유지
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
        self.log.info(f"🚀 [S3ParquetToPostgresOperator] 시작: {self.from_date} ~ {self.to_date}")
        
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

                # 컬럼 정의
                target_columns = [
                    'VENDOR_ID', 'TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME', 
                    'PASSENGER_COUNT', 'TRIP_DISTANCE', 'RATE_CODE_ID', 
                    'STORE_AND_FWD_FLAG', 'PULOCATION_ID', 'DOLOCATION_ID', 
                    'PAYMENT_TYPE', 'FARE_AMOUNT', 'EXTRA', 'MTA_TAX', 
                    'TIP_AMOUNT', 'TOLLS_AMOUNT', 'IMPROVEMENT_SURCHARGE', 
                    'TOTAL_AMOUNT', 'CONGESTION_SURCHARGE', 'AIRPORT_FEE'
                ]
                
                # [핵심 변경] Postgres는 변수를 %s 로 받습니다.
                placeholders = ', '.join(['%s'] * len(target_columns))
                insert_sql = f"INSERT INTO {self.target_table} ({', '.join(target_columns)}) VALUES ({placeholders})"

                total_rows = 0
                for batch in parquet_file.iter_batches(batch_size=self.batch_size):
                    df_chunk = batch.to_pandas()
                    
                    # 컬럼 매핑
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
                    # PostgresHook은 튜플 리스트를 좋아합니다
                    rows = [tuple(x) for x in df_chunk.to_numpy()]
                    
                    cursor.executemany(insert_sql, rows)
                    total_rows += len(rows)
                    
                    del df_chunk, rows
                    gc.collect()

                conn.commit()
                self.log.info(f"✅ {year}-{month} 처리 완료: {total_rows}건 적재됨")
                
                current_dt = current_dt.add(months=1)

        except Exception as e:
            conn.rollback()
            self.log.error(f"❌ 에러 발생: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()