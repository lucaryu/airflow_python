from airflow import DAG
from airflow.models.param import Param
from operators.oracle_to_s3 import OracleToS3ParquetOperator
from operators.s3_to_postgres import S3ParquetToPostgresOperator
import pendulum
from datetime import timedelta

# =========================================================
# 📝 [개발자 영역] 설정
# =========================================================

# 1. Oracle 조회 쿼리 (날짜 변수 없음 -> 전체 조회)
SOURCE_SQL = """
    SELECT SALES_REGION_CODE
     , SALES_REGION_EN
     , SALES_REGION_AR
     , SALES_REGION_CS
     , SALES_REGION_DA
     , SALES_REGION_DE
     , SALES_REGION_EL
     , SALES_REGION_ES
     , SALES_REGION_FI
     , SALES_REGION_FR
     , SALES_REGION_HR
     , SALES_REGION_HU
     , SALES_REGION_ID
     , SALES_REGION_IT
     , SALES_REGION_JA
     , SALES_REGION_KK
     , SALES_REGION_KO
     , SALES_REGION_MS
     , SALES_REGION_NL
     , SALES_REGION_NO
     , SALES_REGION_PL
     , SALES_REGION_PT
     , SALES_REGION_RO
     , SALES_REGION_RU
     , SALES_REGION_SC
     , SALES_REGION_SL
     , SALES_REGION_SV
     , SALES_REGION_TC
     , SALES_REGION_TH
     , SALES_REGION_TR
     , SYSDATE AS ETL_CRY_DTM
  FROM GOSALES.SALES_REGION
"""

# 2. 적재 테이블 이름
TARGET_TABLE = "sales_region"

# 3. 날짜 기준 컬럼
# - 값이 있으면 (예: 'REG_DATE'): 기간별 DELETE 후 적재
# - 값이 없으면 (None): 전체 TRUNCATE 후 적재 (단, 날짜 파라미터가 없어야 함)
DATE_COLUMN = None  # 마스터 테이블이므로 None

# =========================================================

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
    'execution_timeout': timedelta(hours=5)
}

# UI 파라미터 설정 (날짜를 비울 수 있게 type에 null 추가)
params = {
    "from_date": Param(None, type=["string", "null"], description="시작일 (비우면 Full Load)"),
    "to_date": Param(None, type=["string", "null"], description="종료일 (비우면 Full Load)"),
    "target_table": Param(TARGET_TABLE, type="string", description="Postgres 적재 테이블명")
}

with DAG(
    dag_id='dag_oracle23ai_GOSALES_SALES_REGION_20260219_143716',
    default_args=default_args,
    schedule=None,
    params=params,
    tags=['portfolio', 'oracle', 's3', 'postgres', 'hybrid_config'],
) as dag:

    # 1. Oracle -> S3
    extract_task = OracleToS3ParquetOperator(
        task_id='extract_oracle_to_s3',
        oracle_conn_id='oracle_conn',
        s3_conn_id='minio_conn',
        bucket_name='bronze',
        
        oracle_sql=SOURCE_SQL,
        
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        s3_key_prefix='{{ params.target_table | lower }}'
    )

    # 2. S3 -> Postgres
    load_task = S3ParquetToPostgresOperator(
        task_id='load_s3_to_postgres',
        postgres_conn_id='postgres_default',
        minio_conn_id='minio_conn',
        bucket_name='bronze',
        
        target_table='{{ params.target_table }}',
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        key_prefix='{{ params.target_table | lower }}',
        
        # ▼ 상단에서 정의한 변수를 넘겨줍니다.
        date_column=DATE_COLUMN,
        
        batch_size=100000
    )

    extract_task >> load_task