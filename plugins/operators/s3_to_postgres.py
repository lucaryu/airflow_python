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
    1. from_date/to_date가 없으면 (Full Load):
       - 테이블 TRUNCATE (전체 삭제)
       - S3 폴더 내 모든 Parquet 파일 적재
    2. from_date/to_date가 있으면 (Incremental Load):
       - date_column이 있으면 해당 기간 데이터 DELETE (부분 삭제)
       - 해당 기간의 S3 파일만 적재
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
        # 1. 문자열 컬럼 처리 (NULL -> 'N', 공백 제거)
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].fillna('N').astype(str).str.strip()
        
        # 2. 숫자형 NULL 처리 (0으로 채움)
        df = df.fillna(0)
        
        # 3. 날짜 컬럼 자동 감지 및 변환
        for col in df.columns:
            if 'TIME' in col.upper() or 'DATE' in col.upper():
                 df[col] = pd.to_datetime(df[col], errors='coerce')
        return df

    def execute(self, context):
        conn = self._get_postgres_conn()
        cursor = conn.cursor()
        s3_hook = S3Hook(aws_conn_id=self.minio_conn_id)

        try:
            # ✅ 날짜 파라미터 유무 확인
            # Airflow param이 비어있으면 None 또는 빈 문자열('')로 들어옴
            has_date = (self.from_date and str(self.from_date).strip()) and \
                       (self.to_date and str(self.to_date).strip())

            # =========================================================
            # CASE 1: Full Load (날짜 없음 -> TRUNCATE -> 모든 파일)
            # =========================================================
            if not has_date:
                self.log.info(f"📦 [Full Load] 날짜 범위 없음 -> 테이블({self.target_table}) TRUNCATE 실행")
                cursor.execute(f"TRUNCATE TABLE {self.target_table}")
                conn.commit()

                # S3 해당 폴더(prefix) 밑의 모든 파일 조회
                self.log.info(f"📂 S3 전체 스캔 중: {self.key_prefix}/")
                all_objs = s3_hook.list_keys(bucket_name=self.bucket_name, prefix=self.key_prefix)
                
                # .parquet 파일만 필터링
                target_files = [f for f in all_objs if f.endswith('.parquet')] if all_objs else []
                
                if not target_files:
                    self.log.warning("⚠️ 적재할 S3 파일이 하나도 없습니다.")
                    return

                self.log.info(f"총 {len(target_files)}개의 파일을 발견했습니다. 순차 적재 시작.")
                for file_key in target_files:
                    self._load_single_file(s3_hook, cursor, file_key, conn)

            # =========================================================
            # CASE 2: Incremental Load (날짜 있음 -> DELETE -> 기간 파일)
            # =========================================================
            else:
                self.log.info(f"🔄 [Incremental Load] 기간: {self.from_date} ~ {self.to_date}")
                
                # 날짜 파싱
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
                    
                    # 1. 기존 데이터 삭제 (date_column이 있을 때만)
                    if self.date_column:
                        next_month = current_dt.add(months=1).format('YYYY-MM-01')
                        current_month_start = current_dt.format('YYYY-MM-01')
                        
                        delete_sql = f"""
                            DELETE FROM {self.target_table} 
                            WHERE {self.date_column} >= '{current_month_start}' 
                              AND {self.date_column} < '{next_month}'
                        """
                        self.log.info(f"🧹 기간 삭제 실행 ({year}-{month})")
                        cursor.execute(delete_sql)
                    else:
                        self.log.info(f"ℹ️ date_column 없음 -> 삭제 건너뜀 ({year}-{month})")

                    # 2. 해당 월 파일 적재
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
        """파일 하나를 COPY 명령어로 적재하는 내부 함수"""
        self.log.info(f"📥 적재 시작: {file_key}")
        
        file_obj = s3_hook.get_key(key=file_key, bucket_name=self.bucket_name)
        data_stream = io.BytesIO(file_obj.get()['Body'].read())
        parquet_file = pq.ParquetFile(data_stream)
        
        target_columns = parquet_file.schema.names
        
        total_rows = 0
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
            
        conn.commit()
        self.log.info(f"✅ 적재 완료: {total_rows}건")