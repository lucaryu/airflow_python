from airflow import DAG
from airflow.models.param import Param
from operators.oracle_to_s3 import OracleToS3ParquetOperator
from operators.s3_to_postgres import S3ParquetToPostgresOperator
import pendulum
from datetime import timedelta

# =========================================================
# 📝 [개발자 영역] SQL 작성
# =========================================================
# 여기에 실행할 쿼리를 작성하세요.
# - 분할 적재: {start_date}, {end_date} 포함 필수
# - 전체 적재: 날짜 변수 없이 작성
SOURCE_SQL = """
    SELECT PRODUCT_TYPE_CODE
     , PRODUCT_LINE_CODE
     , PRODUCT_TYPE_EN
     , SYSDATE
  FROM GOSALES.PRODUCT_TYPE
"""
# =========================================================

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
    'execution_timeout': timedelta(hours=5)
}

# UI 파라미터 설정
params = {
    "from_date": Param("20230101", type="string", description="시작일 (YYYYMMDD)"),
    "to_date": Param("20230331", type="string", description="종료일 (YYYYMMDD)"),
    
    # ▼▼▼ 여기서 테이블 이름을 입력받습니다. ▼▼▼
    "target_table": Param("product_type", type="string", description="Postgres 적재 테이블명 (S3 폴더명으로도 사용됨)")
}

with DAG(
    dag_id='dag_oracle23ai_GOSALES_PRODUCT_TYPE_20260211_153106',
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
        
        # 개발자가 작성한 SQL 사용
        oracle_sql=SOURCE_SQL,
        
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        
        # ▼▼▼ 입력받은 테이블 이름을 소문자로 바꿔서 폴더명으로 사용 (예: TAXI_DATA -> taxi_data)
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
        
        # ▼▼▼ [추가] 중요! Extract 단계에서 썼던 변수를 여기서도 넘겨줘야 합니다 ▼▼▼
        # DATE_COLUMN 변수가 None이면 Full Load(Truncate) 모드로 동작합니다.
        date_column=DATE_COLUMN, 
        
        batch_size=100000
    )

    extract_task >> load_task