from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import pendulum
import io

# 1. DAG 설정
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
}

# 2. 함수 정의
def download_and_upload_to_minio():
    # NYC Taxi 데이터 (2023년 1월 Yellow Taxi)
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    filename = "yellow_tripdata_2023-01.parquet"
    bucket_name = "bronze"
    key = f"taxi/year=2023/month=01/{filename}"

    print(f"다운로드 시작: {url}")
    
    # 스트리밍 다운로드
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Airflow Connection ID 사용
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    
    # 버킷 없으면 생성
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name)
    
    # 메모리에서 바로 업로드
    file_obj = io.BytesIO(response.content)
    s3_hook.load_file_obj(
        file_obj=file_obj,
        key=key,
        bucket_name=bucket_name,
        replace=True
    )
    
    print(f"업로드 완료: s3://{bucket_name}/{key}")

# 3. DAG 정의
with DAG(
    dag_id='01_ingest_taxi_data',
    default_args=default_args,
    schedule=None,  # 👈 여기가 변경되었습니다! (schedule_interval -> schedule)
    tags=['portfolio', 'ingestion'],
) as dag:

    task_upload = PythonOperator(
        task_id='upload_to_minio',
        python_callable=download_and_upload_to_minio
    )