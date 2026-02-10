from airflow import DAG
from airflow.models.param import Param
from operators.oracle_to_s3 import OracleToS3ParquetOperator
from operators.s3_to_postgres import S3ParquetToPostgresOperator
import pendulum
from datetime import timedelta

# ---------------------------------------------------------
# 1. 개발자가 수정하는 영역 (SQL 정의)
# ---------------------------------------------------------
# Oracle에서 조회할 쿼리를 여기에 직접 작성하세요.
# 주의: 마지막에 WHERE 절로 날짜 필터링이 자동으로 붙으므로, ; (세미콜론)을 쓰지 마세요.
SOURCE_SQL = """
    SELECT 
        VENDOR_ID,
        TPEP_PICKUP_DATETIME,
        TPEP_DROPOFF_DATETIME,
        PASSENGER_COUNT,
        TRIP_DISTANCE,
        RATE_CODE_ID,
        STORE_AND_FWD_FLAG,
        PULOCATION_ID,
        DOLOCATION_ID,
        PAYMENT_TYPE,
        FARE_AMOUNT,
        EXTRA,
        MTA_TAX,
        TIP_AMOUNT,
        TOLLS_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        TOTAL_AMOUNT,
        CONGESTION_SURCHARGE,
        AIRPORT_FEE
    FROM TAXI_DATA
"""

# 기간 조회의 기준이 될 Oracle 컬럼명
DATE_COLUMN = "TPEP_PICKUP_DATETIME"

# ---------------------------------------------------------
# 2. DAG 설정
# ---------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
    'execution_timeout': timedelta(hours=5)
}

# UI에서는 날짜와 타겟 테이블 이름만 입력받음
params = {
    "from_date": Param("20230101", type="string", description="시작일 (YYYYMMDD)"),
    "to_date": Param("20230331", type="string", description="종료일 (YYYYMMDD)"),
    "target_table": Param("TAXI_DATA", type="string", description="Postgres 적재 테이블명")
}

with DAG(
    dag_id='08_taxi_migration',
    default_args=default_args,
    schedule=None,
    params=params,
    tags=['portfolio', 'oracle', 's3', 'postgres', 'code_based_sql'],
) as dag:

    # 1. Oracle -> S3 (Extract)
    extract_task = OracleToS3ParquetOperator(
        task_id='extract_oracle_to_s3',
        oracle_conn_id='oracle_conn',
        s3_conn_id='minio_conn',
        bucket_name='bronze',
        
        # ▼▼▼ [핵심] 개발자가 위에 정의한 변수를 직접 사용 ▼▼▼
        oracle_sql=SOURCE_SQL,
        date_column=DATE_COLUMN,
        
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        s3_key_prefix='taxi_migration'
    )

    # 2. S3 -> Postgres (Load)
    load_task = S3ParquetToPostgresOperator(
        task_id='load_s3_to_postgres',
        postgres_conn_id='postgres_default',
        minio_conn_id='minio_conn',
        bucket_name='bronze',
        
        target_table='{{ params.target_table }}',
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        key_prefix='taxi_migration',
        batch_size=100000
    )

    extract_task >> load_task