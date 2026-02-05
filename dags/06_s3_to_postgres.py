from airflow import DAG
from airflow.models.param import Param
from operators.s3_to_postgres import S3ParquetToPostgresOperator
import pendulum
from datetime import timedelta

params = {
    "from_date": Param("20230101", type="string", description="시작일 (YYYYMMDD)"),
    "to_date": Param("20230331", type="string", description="종료일 (YYYYMMDD)"),
    "tablename": Param("TAXI_DATA", type="string", description="적재할 Postgres 테이블명")
}

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
    'execution_timeout': timedelta(hours=5)
}

with DAG(
    dag_id='06_s3_to_postgres',
    default_args=default_args,
    schedule=None,
    params=params, 
    tags=['portfolio', 'postgres', 'elt'],
) as dag:

    load_task = S3ParquetToPostgresOperator(
        task_id='load_s3_to_postgres',
        
        # Postgres 연결 ID (Airflow Admin -> Connections에서 확인 필요)
        # 기본 설치된 Postgres라면 'postgres_default' 또는 'airflow_db' 등을 사용
        postgres_conn_id='postgres_default', 
        minio_conn_id='minio_conn',
        bucket_name='bronze',
        
        target_table='{{ params.tablename }}',
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        
        batch_size=100000
    )