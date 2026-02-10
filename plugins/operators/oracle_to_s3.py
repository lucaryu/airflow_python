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
    Oracle에서 SQL 결과(SELECT)를 조회하여 S3에 Parquet로 저장
    - oracle_sql: 실행할 전체 SQL (단, 날짜 부분은 {start_date}, {end_date} 로 작성해야 함)
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'oracle_sql')

    def __init__(
        self,
        oracle_conn_id,
        s3_conn_id,
        oracle_sql,       # 전체 SQL을 받음
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
        self.oracle_sql = oracle_sql
        # date_column 삭제됨 (SQL에 직접 작성하므로 불필요)
        self.bucket_name = bucket_name
        self.from_date = from_date
        self.to_date = to_date
        self.s3_key_prefix = s3_key_prefix

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
        oracle_conn = self._get_oracle_conn()

        try:
            while current_dt <= end_dt:
                year = current_dt.format('YYYY')
                month = current_dt.format('MM')
                
                # 날짜 문자열 계산
                current_month_str = current_dt.format('YYYY-MM-01')
                next_month_str = current_dt.add(months=1).format('YYYY-MM-01')
                
                # ▼▼▼ [핵심 변경] 사용자가 준 SQL에 날짜 변수만 매핑 ▼▼▼
                # 사용자가 SQL에 {start_date}와 {end_date}를 적어두면 여기서 치환됩니다.
                try:
                    sql = self.oracle_sql.format(
                        start_date=current_month_str,
                        end_date=next_month_str
                    )
                except KeyError as e:
                    self.log.error("❌ SQL에 {start_date} 또는 {end_date} 포맷 문자열이 없습니다!")
                    raise e
                # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
                
                self.log.info(f"🔍 Oracle 조회 실행 ({year}-{month})")
                self.log.debug(f"실행 SQL: {sql}")
                
                df = pd.read_sql(sql, oracle_conn)
                
                if df.empty:
                    self.log.warning(f"⚠️ 데이터 없음 (Skip): {year}-{month}")
                else:
                    parquet_buffer = io.BytesIO()
                    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                    parquet_buffer.seek(0)
                    
                    filename = f"yellow_tripdata_{year}-{month}.parquet"
                    s3_key = f"{self.s3_key_prefix}/year={year}/month={month}/{filename}"
                    
                    s3_hook.load_bytes(
                        bytes_data=parquet_buffer.getvalue(),
                        key=s3_key,
                        bucket_name=self.bucket_name,
                        replace=True
                    )
                    self.log.info(f"✅ S3 업로드 완료: {s3_key} ({len(df)}건)")

                current_dt = current_dt.add(months=1)
                
        finally:
            if oracle_conn:
                oracle_conn.close()