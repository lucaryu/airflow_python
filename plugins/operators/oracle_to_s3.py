from airflow.models import BaseOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import pendulum
import io
import oracledb

class OracleToS3ParquetOperator(BaseOperator):
    """
    [万能 Custom Operator]
    Oracle -> S3 Parquet 저장
    - date_column이 있으면: 월별 분할 적재 (Incremental)
    - date_column이 없으면: 한 번에 전체 적재 (Full Load)
    """
    
    template_fields = ('from_date', 'to_date', 'bucket_name', 'oracle_sql', 'date_column')

    def __init__(
        self,
        oracle_conn_id,
        s3_conn_id,
        oracle_sql,       # 실행할 SQL (테이블명 또는 쿼리)
        bucket_name,
        from_date,
        to_date,
        date_column=None, # [선택] 없으면 Full Load
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
        self.date_column = date_column # None이면 전체 적재
        self.s3_key_prefix = s3_key_prefix

    def _get_oracle_conn(self):
        oracle_hook = OracleHook(oracle_conn_id=self.oracle_conn_id)
        conn_info = oracle_hook.get_connection(self.oracle_conn_id)
        service_name = conn_info.schema if conn_info.schema else 'Oracle23ai'
        dsn = f"{conn_info.host}:{conn_info.port}/{service_name}"
        return oracledb.connect(user=conn_info.login, password=conn_info.password, dsn=dsn)

    def execute(self, context):
        self.log.info(f"🚀 [OracleToS3] 시작: {self.from_date} ~ {self.to_date}")
        
        # 날짜 파싱
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
            # CASE 1: 날짜 컬럼이 있는 경우 (월별 반복 적재)
            # ---------------------------------------------------------
            if self.date_column and self.date_column.lower() != 'none' and self.date_column.strip() != '':
                self.log.info(f"🔄 모드: 월별 분할 적재 (기준 컬럼: {self.date_column})")
                
                current_dt = start_dt
                while current_dt <= end_dt:
                    year = current_dt.format('YYYY')
                    month = current_dt.format('MM')
                    
                    # 날짜 조건 생성
                    next_month = current_dt.add(months=1).format('YYYY-MM-01')
                    current_month_str = current_dt.format('YYYY-MM-01')
                    
                    sql = f"""
                        SELECT * FROM ({self.oracle_sql}) 
                        WHERE {self.date_column} >= TO_DATE('{current_month_str}', 'YYYY-MM-DD')
                          AND {self.date_column} < TO_DATE('{next_month}', 'YYYY-MM-DD')
                    """
                    
                    self._process_and_upload(oracle_conn, s3_hook, sql, year, month)
                    current_dt = current_dt.add(months=1)

            # ---------------------------------------------------------
            # CASE 2: 날짜 컬럼이 없는 경우 (Full Load - 한 번만 실행)
            # ---------------------------------------------------------
            else:
                self.log.info("📦 모드: 전체 통적재 (Full Load)")
                
                # Full Load는 날짜 조건 없이 원본 SQL 그대로 실행
                sql = f"SELECT * FROM ({self.oracle_sql})"
                
                # 저장 위치는 편의상 from_date의 연/월 폴더에 저장 (Loader가 찾기 쉽게)
                year = start_dt.format('YYYY')
                month = start_dt.format('MM')
                
                self._process_and_upload(oracle_conn, s3_hook, sql, year, month)

        finally:
            if oracle_conn:
                oracle_conn.close()

    def _process_and_upload(self, conn, s3_hook, sql, year, month):
        """데이터 조회 및 S3 업로드 공통 함수"""
        self.log.info(f"🔍 조회 실행: {year}-{month}")
        df = pd.read_sql(sql, conn)
        
        if df.empty:
            self.log.warning(f"⚠️ 데이터 없음 (Skip): {year}-{month}")
            return

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)
        
        # 파일명 통일 (Loader 호환성 유지)
        filename = f"yellow_tripdata_{year}-{month}.parquet"
        s3_key = f"{self.s3_key_prefix}/year={year}/month={month}/{filename}"
        
        s3_hook.load_bytes(
            bytes_data=parquet_buffer.getvalue(),
            key=s3_key,
            bucket_name=self.bucket_name,
            replace=True
        )
        self.log.info(f"✅ S3 업로드 완료: {s3_key} ({len(df)}건)")