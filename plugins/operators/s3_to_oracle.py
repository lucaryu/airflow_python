from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
import pendulum
import io
import oracledb
import pyarrow.parquet as pq
import gc
import time

class S3ParquetToOracleOperator(BaseOperator):
    """
    [Custom Operator]
    S3(MinIO)의 Parquet 파일을 기간(From-To)만큼 읽어 Oracle에 적재
    특징: 전처리 로직 내장 (ORA-01722 및 DPY-3013 방지)
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'target_table')

    def __init__(
        self,
        oracle_conn_id,
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
        self.oracle_conn_id = oracle_conn_id
        self.minio_conn_id = minio_conn_id
        self.target_table = target_table
        self.bucket_name = bucket_name
        self.from_date = from_date
        self.to_date = to_date
        self.key_prefix = key_prefix
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
        """
        [전처리 로직 수정]
        - STORE_AND_FWD_FLAG: 문자열(VARCHAR)이므로 NULL -> 'N'
        - VENDOR_ID 등 ID 컬럼: 숫자형(NUMBER)이므로 NULL -> 0 (리스트에서 제거함)
        """
        
        # ▼ [수정] 진짜 문자열 컬럼만 남깁니다. (나머지는 자동으로 0 처리됨)
        str_cols = ['STORE_AND_FWD_FLAG']
        
        for col in str_cols:
            if col in df.columns:
                # NULL -> 'N', 그리고 강제 문자열 변환
                df[col] = df[col].fillna('N').astype(str).str.strip()

        # 나머지는 숫자형으로 가정하고 NULL -> 0 처리
        # (VENDOR_ID, PULOCATION_ID 등이 여기서 0으로 변환되어 ORA-01722 방지)
        df = df.fillna(0)
        
        # 날짜 컬럼 변환
        date_cols = ['TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df

    def execute(self, context):
        self.log.info(f"🚀 [S3ParquetToOracleOperator] 시작: {self.from_date} ~ {self.to_date}")
        self.log.info(f"🎯 타겟 테이블: {self.target_table}")
        
        conn = self._get_oracle_conn()
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
                
                insert_sql = f"""
                INSERT INTO {self.target_table} ({', '.join(target_columns)}) 
                VALUES ({', '.join([':' + str(i+1) for i in range(len(target_columns))])})
                """

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