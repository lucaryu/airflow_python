from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests
import pendulum
import io

# 1. DAG 설정
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
}

# 2. 함수 정의
def download_and_upload_to_minio(**kwargs):
    # Airflow 실행 시 입력받은 설정값(conf) 가져오기
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf or {}
    
    # 입력값이 없으면 기본값 사용
    from_str = conf.get('from_date', '20230101')
    to_str = conf.get('to_date', '20230101')
    
    # 날짜 변환
    start_date = pendulum.from_format(from_str, 'YYYYMMDD')
    end_date = pendulum.from_format(to_str, 'YYYYMMDD')
    
    print(f"작업 기간: {start_date.to_date_string()} ~ {end_date.to_date_string()}")

    s3_hook = S3Hook(aws_conn_id='minio_conn')
    bucket_name = "bronze"
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name)

    # --- [수정된 부분] While 문으로 월 단위 반복 처리 ---
    current_date = start_date
    processed_months = set()

    while current_date <= end_date:
        year = current_date.format('YYYY')
        month = current_date.format('MM')
        key_ym = f"{year}-{month}"
        
        # 중복된 월(Month)은 건너뛰기 (예: 1월 1일 ~ 1월 31일이면 한 번만 실행)
        if key_ym in processed_months:
            current_date = current_date.add(months=1)
            continue
            
        processed_months.add(key_ym)

        # 다운로드 및 업로드
        filename = f"yellow_tripdata_{year}-{month}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
        key = f"taxi/year={year}/month={month}/{filename}"
        
        print(f"[{year}년 {month}월] 처리 시작... URL: {url}")
        
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 404:
                print(f"  -> 데이터가 없습니다 (404). 스킵합니다.")
            else:
                response.raise_for_status()
                
                file_obj = io.BytesIO(response.content)
                s3_hook.load_file_obj(
                    file_obj=file_obj,
                    key=key,
                    bucket_name=bucket_name,
                    replace=True
                )
                print(f"  -> 업로드 성공: s3://{bucket_name}/{key}")
            
        except Exception as e:
            print(f"  -> 에러 발생: {str(e)}")
            raise e
        
        # [중요] 다음 달 1일로 이동 (무한 루프 방지)
        current_date = current_date.add(months=1)

# 3. DAG 정의
with DAG(
    dag_id='01_ingest_texi_date_yyyymmdd',
    default_args=default_args,
    schedule=None,
    tags=['portfolio', 'ingestion'],
) as dag:

    task_upload = PythonOperator(
        task_id='upload_to_minio',
        python_callable=download_and_upload_to_minio,
    )