from airflow import DAG
from airflow.models.param import Param
from operators.oracle_to_s3 import OracleToS3ParquetOperator
from operators.s3_to_postgres import S3ParquetToPostgresOperator
import pendulum
from datetime import timedelta

# 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
    'execution_timeout': timedelta(hours=5)
}

# 파라미터 정의 (SQL 변경 가능)
params = {
    "from_date": Param("20230101", type="string", description="시작일 (YYYYMMDD)"),
    "to_date": Param("20230331", type="string", description="종료일 (YYYYMMDD)"),
    
    # ▼▼▼ [핵심] 여기서 원하는 SQL을 적습니다. ▼▼▼
    "source_sql": Param("SELECT * FROM TAXI_DATA", type="string", description="Oracle 조회 쿼리"),
    "date_col": Param("TPEP_PICKUP_DATETIME", type="string", description="기간 조회 기준 컬럼명"),
    "target_table": Param("TAXI_DATA", type="string", description="Postgres 적재 테이블명")
}

with DAG(
    dag_id='08_taxi_migration',
    default_args=default_args,
    schedule=None,
    params=params,
    tags=['portfolio', 'oracle', 's3', 'postgres', 'sql_based'],
) as dag:

    # 1. Oracle -> S3 (SQL 기반 추출)
    extract_task = OracleToS3ParquetOperator(
        task_id='extract_oracle_to_s3',
        oracle_conn_id='oracle_conn',
        s3_conn_id='minio_conn',
        bucket_name='bronze',
        
        # ▼ SQL과 날짜 컬럼을 파라미터로 전달
        oracle_sql='{{ params.source_sql }}',
        date_column='{{ params.date_col }}',
        
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
        key_prefix='taxi_migration', # 위에서 저장한 경로와 일치시킴
        batch_size=100000
    )

    extract_task >> load_task