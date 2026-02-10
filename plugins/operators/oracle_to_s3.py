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
    - oracle_sql: 실행할 기본 조회 쿼리 (예: SELECT * FROM TAXI_DATA)
    - date_column: 기간별 분할 기준이 되는 날짜 컬럼명 (예: TPEP_PICKUP_DATETIME)
    """
    
    # 템플릿 변수 허용 (SQL 내부에 {{ ds }} 등을 쓸 수 있음)
    template_fields = ('from_date', 'to_date', 'bucket_name', 'oracle_sql', 'date_column')

    def __init__(
        self,
        oracle_conn_id,
        s3_conn_id,
        oracle_sql,       # [변경] 테이블명 대신 SQL을 받음
        date_column,      # [추가] 날짜 기준 컬럼명
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
        self.date_column = date_column
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
                
                next_month = current_dt.add(months=1).format('YYYY-MM-01')
                current_month_str = current_dt.format('YYYY-MM-01')
                
                # ▼▼▼ [핵심 변경] 입력받은 SQL을 서브쿼리로 감싸고 날짜 조건 추가 ▼▼▼
                # 이렇게 하면 사용자가 "SELECT A, B FROM TABLE" 이라고만 입력해도
                # 자동으로 날짜 필터링이 붙습니다.
                sql = f"""
                    SELECT * FROM ({self.oracle_sql}) 
                    WHERE {self.date_column} >= TO_DATE('{current_month_str}', 'YYYY-MM-DD')
                      AND {self.date_column} < TO_DATE('{next_month}', 'YYYY-MM-DD')
                """
                # ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
                
                self.log.info(f"🔍 Oracle 조회 실행 ({year}-{month})")
                self.log.debug(f"실행 SQL: {sql}")
                
                df = pd.read_sql(sql, oracle_conn)
                
                if df.empty:
                    self.log.warning(f"⚠️ 데이터 없음 (Skip): {year}-{month}")
                else:
                    parquet_buffer = io.BytesIO()
                    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                    parquet_buffer.seek(0)
                    
                    # Postgres 적재 호환성을 위해 yellow_tripdata 이름 유지
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