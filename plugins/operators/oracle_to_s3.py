from airflow.models import BaseOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import pendulum
import io
import oracledb

class OracleToS3ParquetOperator(BaseOperator):
    """
    [Custom Operator]
    Oracle 데이터를 조회하여 S3(MinIO)에 Parquet 포맷으로 저장
    경로 패턴: {key_prefix}/year=YYYY/month=MM/파일.parquet
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'oracle_table')

    def __init__(
        self,
        oracle_conn_id,
        s3_conn_id,
        oracle_table,
        bucket_name,
        from_date,
        to_date,
        s3_key_prefix='taxi',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.s3_conn_id = s3_conn_id
        self.oracle_table = oracle_table
        self.bucket_name = bucket_name
        self.from_date = from_date
        self.to_date = to_date
        self.s3_key_prefix = s3_key_prefix

    def _get_oracle_conn(self):
        oracle_hook = OracleHook(oracle_conn_id=self.oracle_conn_id)
        # SQLAlchemy 엔진 대신 raw connection 사용 (pandas read_sql용)
        conn = oracle_hook.get_conn()
        return conn

    def execute(self, context):
        self.log.info(f"🚀 [OracleToS3] 시작: {self.from_date} ~ {self.to_date}")
        
        # 날짜 파싱
        try:
            start_dt = pendulum.from_format(str(self.from_date), 'YYYYMMDD')
            end_dt = pendulum.from_format(str(self.to_date), 'YYYYMMDD')
        except ValueError:
            start_dt = pendulum.parse(str(self.from_date))
            end_dt = pendulum.parse(str(self.to_date))

        current_dt = start_dt
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        oracle_conn = self._get_oracle_conn()

        try:
            while current_dt <= end_dt:
                year = current_dt.format('YYYY')
                month = current_dt.format('MM')
                
                # 월별 데이터 조회 쿼리 (TPEP_PICKUP_DATETIME 기준)
                next_month = current_dt.add(months=1).format('YYYY-MM-01')
                current_month_str = current_dt.format('YYYY-MM-01')
                
                sql = f"""
                    SELECT * FROM {self.oracle_table}
                    WHERE TPEP_PICKUP_DATETIME >= TO_DATE('{current_month_str}', 'YYYY-MM-DD')
                      AND TPEP_PICKUP_DATETIME < TO_DATE('{next_month}', 'YYYY-MM-DD')
                """
                
                self.log.info(f"🔍 Oracle 조회 중... ({year}-{month})")
                
                # Pandas로 읽기
                df = pd.read_sql(sql, oracle_conn)
                
                if df.empty:
                    self.log.warning(f"⚠️ 데이터 없음 (Skip): {year}-{month}")
                else:
                    # Parquet 변환 (메모리 버퍼 사용)
                    parquet_buffer = io.BytesIO()
                    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                    parquet_buffer.seek(0)
                    
                    # S3 업로드 경로 생성
                    s3_key = f"{self.s3_key_prefix}/year={year}/month={month}/oracle_export_{year}_{month}.parquet"
                    
                    s3_hook.load_bytes(
                        bytes_data=parquet_buffer.getvalue(),
                        key=s3_key,
                        bucket_name=self.bucket_name,
                        replace=True
                    )
                    self.log.info(f"✅ S3 업로드 완료: {s3_key} ({len(df)}건)")

                current_dt = current_dt.add(months=1)
                
        finally:
            oracle_conn.close()