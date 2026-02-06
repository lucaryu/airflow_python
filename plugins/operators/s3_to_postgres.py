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
    S3(MinIO) -> PostgreSQL 초고속 적재 (COPY 명령 사용 + 타입 에러 해결)
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
        # 1. 문자열 컬럼 처리 (NULL -> 'N')
        str_cols = ['STORE_AND_FWD_FLAG', 'VENDOR_ID', 'RATE_CODE_ID', 
                    'PAYMENT_TYPE', 'PULOCATION_ID', 'DOLOCATION_ID']
        
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].fillna('N').astype(str).str.strip()
        
        # 2. 기본 NULL -> 0 처리
        df = df.fillna(0)
        
        # ▼▼▼ [핵심 수정] 정수형(Integer) 컬럼 강제 변환 (1.0 -> 1) ▼▼▼
        # PASSENGER_COUNT가 실수(float)로 되어 있으면 COPY 명령에서 에러 발생
        int_cols = ['PASSENGER_COUNT']
        for col in int_cols:
            if col in df.columns:
                # 안전하게 0으로 채우고 int로 변환
                df[col] = df[col].astype(int)
        # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
        
        # 3. 날짜 컬럼 처리
        date_cols = ['TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df

    def execute(self, context):
        self.log.info(f"🚀 [S3ParquetToPostgresOperator] COPY 적재 시작: {self.from_date} ~ {self.to_date}")
        
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
                
                # 멱등성을 위한 기존 데이터 삭제 (Delete)
                next_month = current_dt.add(months=1).format('YYYY-MM-01')
                current_month_start = current_dt.format('YYYY-MM-01')
                delete_sql = f"DELETE FROM {self.target_table} WHERE TPEP_PICKUP_DATETIME >= '{current_month_start}' AND TPEP_PICKUP_DATETIME < '{next_month}'"
                self.log.info(f"🧹 기존 데이터 삭제 중... ({year}-{month})")
                cursor.execute(delete_sql)

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
                    
                    # 메모리 내 CSV 변환 후 COPY 명령 실행
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

                conn.commit()
                self.log.info(f"✅ {year}-{month} COPY 완료: {total_rows}건 적재됨")
                
                current_dt = current_dt.add(months=1)

        except Exception as e:
            conn.rollback()
            self.log.error(f"❌ 에러 발생: {e}")
            raise e
        finally:
            cursor.close()
            conn.close()