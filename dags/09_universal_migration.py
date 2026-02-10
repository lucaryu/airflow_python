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

# 파라미터 정의
params = {
    "from_date": Param("20230101", type="string", description="시작일 (YYYYMMDD) - Full Load시 저장 폴더명으로 사용됨"),
    "to_date": Param("20230331", type="string", description="종료일 (YYYYMMDD)"),
    
    # ▼ SQL 입력 (테이블명만 써도 되고, SELECT문 써도 됨)
    "source_sql": Param("SELECT * FROM TAXI_DATA", type="string", description="Oracle 조회 쿼리"),
    
    # ▼▼▼ [핵심] 비워두면 Full Load, 입력하면 Partition Load ▼▼▼
    "date_col": Param("", type=["string", "null"], description="[선택] 날짜 컬럼명 (비워두면 전체 통적재)"),
    
    "target_table": Param("TAXI_DATA", type="string", description="Postgres 적재 테이블명")
}

with DAG(
    dag_id='09_universal_migration',
    default_args=default_args,
    schedule=None,
    params=params,
    tags=['portfolio', 'oracle', 's3', 'postgres', 'universal'],
) as dag:

    # 1. Oracle -> S3 (자동 분기 처리)
    extract_task = OracleToS3ParquetOperator(
        task_id='extract_oracle_to_s3',
        oracle_conn_id='oracle_conn',
        s3_conn_id='minio_conn',
        bucket_name='bronze',
        
        oracle_sql='{{ params.source_sql }}',
        date_column='{{ params.date_col }}',  # 이 값이 없으면 Operator가 알아서 Full Load 함
        
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        s3_key_prefix='universal_migration'
    )

    # 2. S3 -> Postgres
    # (Full Load일 경우 파일 하나만 생성되지만, Loader는 기간만큼 파일 탐색을 시도합니다.
    #  하지만 Full Load는 보통 'from_date'의 월 폴더에 저장되므로, 
    #  Loader가 첫 달만 처리하고 나머지는 '파일 없음'으로 스킵하게 되어 자연스럽게 동작합니다.)
    load_task = S3ParquetToPostgresOperator(
        task_id='load_s3_to_postgres',
        postgres_conn_id='postgres_default',
        minio_conn_id='minio_conn',
        bucket_name='bronze',
        
        target_table='{{ params.target_table }}',
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        key_prefix='universal_migration',
        batch_size=100000
    )

    extract_task >> load_task