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
    1. from_date/to_date가 없으면 (Full Load):
       - SQL을 그대로 실행 (치환 없음)
       - S3의 'full_load' 폴더에 저장
    2. from_date/to_date가 있으면 (Incremental Load):
       - SQL 내의 {start_date}, {end_date}를 치환하며 반복 실행
       - S3의 year=YYYY/month=MM 폴더에 저장
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'oracle_sql', 's3_key_prefix')

    def __init__(
        self,
        oracle_conn_id,
        s3_conn_id,
        oracle_sql,
        bucket_name,
        from_date=None,  # None 허용
        to_date=None,    # None 허용
        s3_key_prefix='taxi',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.s3_conn_id = s3_conn_id
        self.oracle_sql = oracle_sql
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

        # ✅ 날짜 파라미터 유무 확인
        has_date = (self.from_date and str(self.from_date).strip() != 'None') and \
                   (self.to_date and str(self.to_date).strip() != 'None')

        oracle_conn = self._get_oracle_conn()
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        try:
            # =========================================================
            # CASE 1: Full Load (날짜 없음 -> SQL 그대로 실행)
            # =========================================================
            if not has_date:
                self.log.info("📦 [Full Load] 날짜 범위 없음 -> SQL 그대로 실행")
                
                # 치환 없이 원본 SQL 실행
                final_sql = self.oracle_sql
                
                # 저장 위치: full_load 폴더 (오늘 날짜 파일명)
                today = pendulum.now('Asia/Seoul').format('YYYYMMDD')
                filename = f"full_load_{today}.parquet"
                s3_key = f"{self.s3_key_prefix}/full_load/{filename}"
                
                self._process_and_upload(oracle_conn, s3_hook, final_sql, s3_key)

            # =========================================================
            # CASE 2: Incremental Load (날짜 있음 -> 반복 실행)
            # =========================================================
            else:
                try:
                    start_dt = pendulum.from_format(str(self.from_date), 'YYYYMMDD')
                    end_dt = pendulum.from_format(str(self.to_date), 'YYYYMMDD')
                except ValueError:
                    start_dt = pendulum.parse(str(self.from_date))
                    end_dt = pendulum.parse(str(self.to_date))
                
                self.log.info("🔄 [Incremental Load] 월별 분할 조회 시작")
                
                current_dt = start_dt
                while current_dt <= end_dt:
                    year = current_dt.format('YYYY')
                    month = current_dt.format('MM')
                    
                    # 날짜 변수 계산
                    current_month_str = current_dt.format('YYYY-MM-01')
                    next_month_str = current_dt.add(months=1).format('YYYY-MM-01')
                    
                    # SQL 치환 ({start_date}, {end_date}가 있을 경우만)
                    if "{start_date}" in self.oracle_sql:
                        final_sql = self.oracle_sql.format(
                            start_date=current_month_str,
                            end_date=next_month_str
                        )
                    else:
                        # 날짜가 있는데 SQL에 변수가 없으면 그냥 실행 (중복 실행 주의)
                        final_sql = self.oracle_sql
                    
                    # 저장 위치: 연/월 폴더
                    filename = f"yellow_tripdata_{year}-{month}.parquet"
                    s3_key = f"{self.s3_key_prefix}/year={year}/month={month}/{filename}"
                    
                    self._process_and_upload(oracle_conn, s3_hook, final_sql, s3_key)
                    current_dt = current_dt.add(months=1)

        finally:
            if oracle_conn:
                oracle_conn.close()

    def _process_and_upload(self, conn, s3_hook, sql, s3_key):
        self.log.info(f"🔍 조회 실행...")
        self.log.debug(f"SQL: {sql}")
        
        df = pd.read_sql(sql, conn)
        
        if df.empty:
            self.log.warning(f"⚠️ 데이터 없음 (Skip)")
            return

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
        
        s3_hook.load_bytes(
            bytes_data=parquet_buffer.getvalue(),
            key=s3_key,
            bucket_name=self.bucket_name,
            replace=True
        )
        self.log.info(f"✅ S3 업로드 완료: {s3_key} ({len(df)}건)")