import sys
import os
from airflow import DAG


# 1. plugins 폴더를 파이썬 라이브러리 경로에 강제로 추가합니다.
# (Docker 환경에서 보통 /opt/airflow/plugins 입니다)
sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'plugins'))

# 2. 이제 에러 없이 import가 될 것입니다.
from operators.http_to_s3_custom import YearMonthRangeToS3Operator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import pendulum
import io

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
}

with DAG(
    dag_id='02_custom_operator_taxi_yyyymmdd_common', # DAG ID 새로 변경
    default_args=default_args,
    schedule=None,
    tags=['portfolio', 'custom_operator'],
) as dag:

    # 복잡한 함수 따위 필요 없음! 바로 Operator 호출
    ingest_task = YearMonthRangeToS3Operator(
        task_id='ingest_taxi_range',
        aws_conn_id='minio_conn',
        bucket_name='bronze',
        
        # {year}, {month} 만 적어주면 오퍼레이터가 알아서 바꿈
        base_url='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet',
        s3_key='taxi/year={year}/month={month}/yellow_tripdata_{year}-{month}.parquet',
        
        # Airflow 설정(conf)에서 받은 값을 템플릿으로 넘김
        from_date='{{ dag_run.conf.get("from_date", "20230101") }}',
        to_date='{{ dag_run.conf.get("to_date", "20230101") }}'
    )