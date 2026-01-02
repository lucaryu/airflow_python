from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import pendulum
import io

# 1. DAG 설정 (작업의 기본 정보)
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    'catchup': False, # 과거 데이터 한꺼번에 돌리지 않기
}

# 2. 함수 정의 (실제로 할 일)
def download_and_upload_to_minio():
    # NYC Taxi 데이터 (2023년 1월 Yellow Taxi)
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    filename = "yellow_tripdata_2023-01.parquet"
    bucket_name = "bronze"
    key = f"taxi/year=2023/month=01/{filename}" # 저장될 경로

    print(f"다운로드 시작: {url}")
    
    # 인터넷에서 파일 스트리밍으로 읽기 (메모리 절약)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Airflow에 등록한 'minio_conn' 정보를 이용해 MinIO 접속
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    
    # MinIO 버킷이 없으면 생성
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name)
    
    # 파일 업로드 (BytesIO를 사용해 파일 저장 없이 메모리에서 바로 전송)
    file_obj = io.BytesIO(response.content)
    s3_hook.load_file_obj(
        file_obj=file_obj,
        key=key,
        bucket_name=bucket_name,
        replace=True
    )
    
    print(f"업로드 완료: s3://{bucket_name}/{key}")

# 3. DAG 정의 (워크플로우 조립)
with DAG(
    dag_id='01_ingest_taxi_data',
    default_args=default_args,
    schedule_interval=None, # 수동으로 실행 (버튼 눌러서)
    tags=['portfolio', 'ingestion'],
) as dag:

    task_upload = PythonOperator(
        task_id='upload_to_minio',
        python_callable=download_and_upload_to_minio
    )