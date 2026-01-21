from airflow import DAG
from airflow.models.param import Param
from operators.s3_to_oracle import S3ParquetToOracleOperator # Custom Operator 로드
import pendulum
from datetime import timedelta

# ---------------------------------------------------------
# 1. UI 입력 파라미터 정의 (YYYYMMDD 형식)
# ---------------------------------------------------------
params = {
    "from_date": Param("20230101", type="string", description="시작일 (YYYYMMDD)"),
    "to_date": Param("20230331", type="string", description="종료일 (YYYYMMDD)"),
}

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
    'execution_timeout': timedelta(hours=5)
}

with DAG(
    dag_id='05_taxi_load_param',
    default_args=default_args,
    schedule=None,
    params=params, # 파라미터 등록
    tags=['portfolio', 'oracle', 'custom_operator'],
) as dag:

    # ---------------------------------------------------------
    # 2. Custom Operator 호출
    # ---------------------------------------------------------
    load_taxi_data = S3ParquetToOracleOperator(
        task_id='load_taxi_range',
        
        # 연결 정보 (고정값)
        oracle_conn_id='oracle_conn',
        minio_conn_id='minio_conn',
        target_table='TAXI_DATA',
        bucket_name='bronze',
        
        # ▼▼▼ [핵심] UI에서 입력한 값을 파라미터로 넘김 ▼▼▼
        from_date='{{ params.from_date }}',
        to_date='{{ params.to_date }}',
        
        # 성능 옵션
        batch_size=100000
    )