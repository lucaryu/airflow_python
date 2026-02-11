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
    [Universal Custom Operator]
    S3(MinIO) -> PostgreSQL 초고속 적재 (COPY 명령 사용)
    - date_column 파라미터가 있으면: 해당 기간 데이터만 삭제 후 적재 (Incremental)
    - date_column 파라미터가 없으면: 테이블 전체 비우고(TRUNCATE) 적재 (Full Load)
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'target_table', 'key_prefix', 'date_column')

    def __init__(
        self,
        postgres_conn_id,
        minio_conn_id,
        target_table,
        bucket_name,
        from_date,
        to_date,
        key_prefix='taxi',
        date_column=None,  # [추가] 날짜 기준 컬럼 (없으면 Full Load)
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
        # 문자열 컬럼 처리
        str_cols = ['STORE_AND_FWD_FLAG', 'VENDOR_ID', 'RATE_CODE_ID', 
                    'PAYMENT_TYPE', 'PULOCATION_ID', 'DOLOCATION_ID']
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].fillna('N').astype(str).str.strip()
        
        df = df.fillna(0)
        
        # 정수형 변환이 필요한 컬럼들 (에러 방지)
        int_cols = ['PASSENGER_COUNT']
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].astype(int)
        
        # 날짜 컬럼 처리
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

            # ---------------------------------------------------------
            # [Full Load 처리] 날짜 컬럼이 없으면 먼저 테이블을 싹 비운다
            # ---------------------------------------------------------
            if not self.date_column:
                self.log.info(f"🧹 Full Load 모드: 테이블({self.target_table}) 전체 비우기 (TRUNCATE)")
                cursor.execute(f"TRUNCATE TABLE {self.target_table}")
                conn.commit()

            current_dt = start_dt
            s3_hook = S3Hook(aws_conn_id=self.minio_conn_id)

            while current_dt <= end_dt:
                year = current_dt.format('YYYY')
                month = current_dt.format('MM')
                
                # ---------------------------------------------------------
                # [Incremental Load 처리] 날짜 컬럼이 있으면 해당 월 데이터만 삭제
                # ---------------------------------------------------------
                if self.date_column:
                    next_month = current_dt.add(months=1).format('YYYY-MM-01')
                    current_month_start = current_dt.format('YYYY-MM-01')
                    
                    delete_sql = f"""
                        DELETE FROM {self.target_table} 
                        WHERE {self.date_column} >= '{current_month_start}' 
                          AND {self.date_column} < '{next_month}'
                    """
                    self.log.info(f"🧹 기존 데이터 삭제 중... ({year}-{month})")
                    cursor.execute(delete_sql)

                # 파일 이름 규칙 (OracleToS3와 동일하게 맞춤)
                filename = f"yellow_tripdata_{year}-{month}.parquet"
                file_key = f"{self.key_prefix}/year={year}/month={month}/{filename}"
                
                self.log.info(f"📂 파일 탐색: {file_key}")
                
                if not s3_hook.check_for_key(file_key, bucket_name=self.bucket_name):
                    self.log.warning(f"⚠️ 파일 없음 (Skip): {file_key}")
                    current_dt = current_dt.add(months=1)
                    continue

                # S3 파일 읽기 및 COPY 적재
                file_obj = s3_hook.get_key(key=file_key, bucket_name=self.bucket_name)
                data_stream = io.BytesIO(file_obj.get()['Body'].read())
                parquet_file = pq.ParquetFile(data_stream)
                
                # Parquet 파일의 컬럼명을 그대로 사용하여 COPY (순서 중요)
                # 첫 번째 배치의 스키마를 읽어서 컬럼 리스트 생성
                schema = parquet_file.schema.names
                target_columns = schema # Parquet 컬럼 순서대로 DB에 넣음
                
                total_rows = 0
                for batch in parquet_file.iter_batches(batch_size=self.batch_size):
                    df_chunk = batch.to_pandas()
                    
                    # 전처리 (NULL 처리, 타입 변환 등)
                    df_chunk = self._preprocess_data(df_chunk)
                    
                    # 메모리 내 CSV 변환
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