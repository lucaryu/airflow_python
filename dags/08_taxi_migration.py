from airflow import DAG
from airflow.models.param import Param
from operators.oracle_to_s3 import OracleToS3ParquetOperator
from operators.s3_to_postgres import S3ParquetToPostgresOperator
import pendulum
from datetime import timedelta

# ---------------------------------------------------------
# 1. 개발자가 작성하는 전체 SQL (WHERE 절 포함)
# ---------------------------------------------------------
# 주의: 날짜 부분은 Operator가 채워주므로 {start_date}, {end_date} 로 적어야 합니다.
# TO_DATE 함수 안의 형식을 꼭 맞춰주세요.
SOURCE_SQL = """
    SELECT 
        *
    FROM TAXI_DATA
    WHERE TPEP_PICKUP_DATETIME >= TO_DATE('{start_date}', 'YYYY-MM-DD')
      AND TPEP_PICKUP_DATETIME <  TO_DATE('{end_date}',   'YYYY-MM-DD')
"""

# ---------------------------------------------------------
# 2. DAG 설정
# ---------------------------------------------------------
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
    'execution_timeout': timedelta(hours=5)
}

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
    tags=['portfolio', 'oracle', 's3', 'postgres', 'full_sql_control'],
) as dag:

    # 1. Oracle -> S3 (전체 SQL 전달)
    extract_task = OracleToS3ParquetOperator(
        task_id='extract_oracle_to_s3',
        oracle_conn_id='oracle_conn',
        s3_conn_id='minio_conn',
        bucket_name='bronze',
        
        # ▼▼▼ 전체 SQL을 그대로 넘깁니다. (date_column 파라미터 삭제됨) ▼▼▼
        oracle_sql=SOURCE_SQL,
        
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        s3_key_prefix='taxi_migration'
    )

    # 2. S3 -> Postgres (고속 적재)
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