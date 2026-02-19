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
    Oracle -> S3 Parquet 저장 (파일명 규칙 변경 적용)
    1. Full Load: {prefix}/{prefix}_full.parquet (덮어쓰기)
    2. Incremental: {prefix}/{YYYY}/{YYYYMM}/{prefix}_{YYYYMM}.parquet
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'oracle_sql', 's3_key_prefix')

    def __init__(
        self,
        oracle_conn_id,
        s3_conn_id,
        oracle_sql,
        bucket_name,
        from_date=None,
        to_date=None,
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

        # 날짜 파라미터 유무 확인
        def is_valid_date(d):
            return d and str(d).strip().lower() not in ['none', '', 'null']
        
        has_date = is_valid_date(self.from_date) and is_valid_date(self.to_date)

        oracle_conn = self._get_oracle_conn()
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)

        try:
            # =========================================================
            # CASE 1: Full Load (파티션 없음 -> _full.parquet)
            # =========================================================
            if not has_date:
                self.log.info("📦 [Full Load] 모드")
                
                final_sql = self.oracle_sql
                
                # [규칙 변경] 테이블명_full.parquet (고정된 이름으로 덮어쓰기)
                filename = f"{self.s3_key_prefix}_full.parquet"
                s3_key = f"{self.s3_key_prefix}/{filename}"
                
                self._process_and_upload(oracle_conn, s3_hook, final_sql, s3_key)

            # =========================================================
            # CASE 2: Incremental Load (파티션 있음 -> YYYY/YYYYMM/...)
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
                    yyyymm = current_dt.format('YYYYMM')
                    
                    current_month_str = current_dt.format('YYYY-MM-01')
                    next_month_str = current_dt.add(months=1).format('YYYY-MM-01')
                    
                    if "{start_date}" in self.oracle_sql:
                        final_sql = self.oracle_sql.format(
                            start_date=current_month_str,
                            end_date=next_month_str
                        )
                    else:
                        final_sql = self.oracle_sql
                    
                    # [규칙 변경] YYYY/YYYYMM/테이블명_YYYYMM.parquet
                    filename = f"{self.s3_key_prefix}_{yyyymm}.parquet"
                    s3_key = f"{self.s3_key_prefix}/{year}/{yyyymm}/{filename}"
                    
                    self._process_and_upload(oracle_conn, s3_hook, final_sql, s3_key)
                    current_dt = current_dt.add(months=1)

        finally:
            if oracle_conn:
                oracle_conn.close()

    def _process_and_upload(self, conn, s3_hook, sql, s3_key):
        self.log.info(f"🔍 조회 및 업로드 대상: {s3_key}")
        
        df = pd.read_sql(sql, conn)
        
        if df.empty:
            self.log.warning(f"⚠️ 데이터 없음 (Skip)")
            return

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
        
        # replace=True 덕분에 같은 이름이면 덮어씌워짐 (중복 해결)
        s3_hook.load_bytes(
            bytes_data=parquet_buffer.getvalue(),
            key=s3_key,
            bucket_name=self.bucket_name,
            replace=True
        )
        self.log.info(f"✅ S3 업로드 완료 ({len(df)}건)")