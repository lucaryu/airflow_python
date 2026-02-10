from airflow.models import BaseOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import pendulum
import io
import oracledb

class OracleToS3ParquetOperator(BaseOperator):
    """
    [Universal Custom Operator]
    Oracle SQL 결과를 S3에 Parquet로 저장
    - oracle_sql에 '{start_date}'가 포함되면 -> 월별 분할 적재 (Incremental)
    - oracle_sql에 '{start_date}'가 없으면 -> 전체 통적재 (Full Load)
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'oracle_sql')

    def __init__(
        self,
        oracle_conn_id,
        s3_conn_id,
        oracle_sql,       # 전체 SQL (날짜 변수 포함 가능)
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
        # date_column 파라미터는 삭제했습니다. (SQL 문자열 파싱으로 대체)
        self.bucket_name = bucket_name
        self.from_date = from_date
        self.to_date = to_date
        self.s3_key_prefix = s3_key_prefix

    def _get_oracle_conn(self):
        oracle_hook = OracleHook(oracle_conn_id=self.oracle_conn_id)
        conn_info = oracle_hook.get_connection(self.oracle_conn_id)
        service_name = conn_info.schema if conn_info.schema else 'Oracle23ai'
        dsn = f"{conn_info.host}:{conn_info.port}/{service_name}"
        return oracledb.connect(user=conn_info.login, password=conn_info.password, dsn=dsn)

    def execute(self, context):
        self.log.info(f"🚀 [OracleToS3] 시작: {self.from_date} ~ {self.to_date}")
        
        try:
            start_dt = pendulum.from_format(str(self.from_date), 'YYYYMMDD')
            end_dt = pendulum.from_format(str(self.to_date), 'YYYYMMDD')
        except ValueError:
            start_dt = pendulum.parse(str(self.from_date))
            end_dt = pendulum.parse(str(self.to_date))

        oracle_conn = self._get_oracle_conn()
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        try:
            # ---------------------------------------------------------
            # CASE 1: SQL 내부에 날짜 변수가 있는 경우 (분할 적재)
            # ---------------------------------------------------------
            if "{start_date}" in self.oracle_sql:
                self.log.info("🔄 모드: 월별 분할 적재 (SQL 내 날짜 변수 감지됨)")
                
                current_dt = start_dt
                while current_dt <= end_dt:
                    year = current_dt.format('YYYY')
                    month = current_dt.format('MM')
                    
                    # 날짜 변수 계산
                    current_month_str = current_dt.format('YYYY-MM-01')
                    next_month_str = current_dt.add(months=1).format('YYYY-MM-01')
                    
                    # ▼ 사용자가 작성한 SQL에 날짜만 채워 넣음 (.format 사용)
                    final_sql = self.oracle_sql.format(
                        start_date=current_month_str,
                        end_date=next_month_str
                    )
                    
                    self._process_and_upload(oracle_conn, s3_hook, final_sql, year, month)
                    current_dt = current_dt.add(months=1)

            # ---------------------------------------------------------
            # CASE 2: SQL 내부에 날짜 변수가 없는 경우 (전체 적재)
            # ---------------------------------------------------------
            else:
                self.log.info("📦 모드: 전체 통적재 (SQL 내 날짜 변수 없음)")
                
                # 변환 없이 그대로 실행 (SELECT * FROM 감싸지 않음!)
                final_sql = self.oracle_sql
                
                # 저장 위치는 시작일 기준 연/월 사용
                year = start_dt.format('YYYY')
                month = start_dt.format('MM')
                
                self._process_and_upload(oracle_conn, s3_hook, final_sql, year, month)

        finally:
            if oracle_conn:
                oracle_conn.close()

    def _process_and_upload(self, conn, s3_hook, sql, year, month):
        self.log.info(f"🔍 조회 실행: {year}-{month}")
        self.log.debug(f"SQL: {sql}")
        
        df = pd.read_sql(sql, conn)
        
        if df.empty:
            self.log.warning(f"⚠️ 데이터 없음 (Skip): {year}-{month}")
            return

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