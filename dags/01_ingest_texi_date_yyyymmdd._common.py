from airflow import DAG
import pendulum
# 방금 만든 커스텀 오퍼레이터 임포트 (경로 주의)
# airflow는 plugins 폴더를 자동으로 스캔하므로 바로 import 가능하거나, 
# 경로가 안 잡히면 sys.path.append를 써야 할 수도 있습니다.
# 보통은 아래처럼 씁니다.
from operators.http_to_s3_custom import YearMonthRangeToS3Operator

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