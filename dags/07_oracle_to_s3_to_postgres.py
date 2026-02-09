from airflow import DAG
from airflow.models.param import Param
from operators.oracle_to_s3 import OracleToS3ParquetOperator
from operators.s3_to_postgres import S3ParquetToPostgresOperator
import pendulum
from datetime import timedelta

# UI 입력 파라미터
params = {
    "from_date": Param("20230101", type="string", description="시작일 (YYYYMMDD)"),
    "to_date": Param("20230331", type="string", description="종료일 (YYYYMMDD)"),
    "oracle_table": Param("TAXI_DATA", type="string", description="소스 Oracle 테이블"),
    "postgres_table": Param("TAXI_DATA", type="string", description="타겟 Postgres 테이블")
}

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
    'execution_timeout': timedelta(hours=5)
}

with DAG(
    dag_id='07_oracle_to_s3_to_postgres',
    default_args=default_args,
    schedule=None,
    params=params, 
    tags=['portfolio', 'migration', 'oracle', 'postgres'],
) as dag:

    # 1. Oracle -> S3 (Extract)
    extract_to_s3 = OracleToS3ParquetOperator(
        task_id='extract_oracle_to_s3',
        oracle_conn_id='oracle_conn',
        s3_conn_id='minio_conn',
        bucket_name='bronze',
        
        oracle_table='{{ params.oracle_table }}',
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        s3_key_prefix='oracle_migration'  # S3 저장 폴더명 구분
    )

    # 2. S3 -> Postgres (Load)
    load_to_postgres = S3ParquetToPostgresOperator(
        task_id='load_s3_to_postgres',
        postgres_conn_id='postgres_default',
        minio_conn_id='minio_conn',
        bucket_name='bronze',
        
        target_table='{{ params.postgres_table }}',
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        key_prefix='oracle_migration', # 위에서 저장한 경로와 맞춰줌
        batch_size=100000
    )

    # 순서 연결: 추출 먼저 하고 적재
    extract_to_s3 >> load_to_postgres