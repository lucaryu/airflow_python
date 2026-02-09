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
        # ▼ [수정] OracleHook은 정보만 가져오고, 연결은 oracledb.connect로 직접 수행
        oracle_hook = OracleHook(oracle_conn_id=self.oracle_conn_id)
        conn_info = oracle_hook.get_connection(self.oracle_conn_id)
        
        # 서비스 이름 추출 (schema가 없으면 기본값 사용)
        service_name = conn_info.schema if conn_info.schema else 'Oracle23ai'
        dsn = f"{conn_info.host}:{conn_info.port}/{service_name}"
        
        # 직접 연결 생성 (성공했던 방식)
        conn = oracledb.connect(
            user=conn_info.login,
            password=conn_info.password,
            dsn=dsn
        )
        return conn

    def execute(self, context):
        self.log.info(f"🚀 [OracleToS3] 시작: {self.from_date} ~ {self.to_date}")
        
        try:
            start_dt = pendulum.from_format(str(self.from_date), 'YYYYMMDD')
            end_dt = pendulum.from_format(str(self.to_date), 'YYYYMMDD')
        except ValueError:
            start_dt = pendulum.parse(str(self.from_date))
            end_dt = pendulum.parse(str(self.to_date))

        current_dt = start_dt
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        
        # DB 연결
        oracle_conn = self._get_oracle_conn()

        try:
            while current_dt <= end_dt:
                year = current_dt.format('YYYY')
                month = current_dt.format('MM')
                
                next_month = current_dt.add(months=1).format('YYYY-MM-01')
                current_month_str = current_dt.format('YYYY-MM-01')
                
                # 날짜 필터링 조회
                sql = f"""
                    SELECT * FROM {self.oracle_table}
                    WHERE TPEP_PICKUP_DATETIME >= TO_DATE('{current_month_str}', 'YYYY-MM-DD')
                      AND TPEP_PICKUP_DATETIME < TO_DATE('{next_month}', 'YYYY-MM-DD')
                """
                
                self.log.info(f"🔍 Oracle 조회 중... ({year}-{month})")
                
                # Pandas로 데이터 읽기 (Chunking 없이 한 번에 읽음 - 월 단위라 괜찮음)
                df = pd.read_sql(sql, oracle_conn)
                
                if df.empty:
                    self.log.warning(f"⚠️ 데이터 없음 (Skip): {year}-{month}")
                else:
                    # Parquet 변환
                    parquet_buffer = io.BytesIO()
                    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                    parquet_buffer.seek(0)
                    
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