from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from operators.s3_to_oracle import S3ToOracleOperator

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='10_s3_csv_to_oracle',
    default_args=default_args,
    schedule=None, # 수동 실행
    catchup=False,
    tags=['s3', 'oracle', 'csv', 'kkbox']
) as dag:

    # =========================================================================
    # [사용 방법]
    # S3ToOracleOperator를 사용하여 MinIO(S3)의 CSV/Parquet 파일을 Oracle로 적재합니다.
    # 해당 타겟 테이블(target_table)은 Oracle DB에 미리 생성되어 있어야 합니다.
    # =========================================================================

    load_train_csv = S3ToOracleOperator(
        task_id='load_train_csv',
        oracle_conn_id='oracle_conn',       # Airflow Connection ID (Oracle)
        minio_conn_id='minio_conn',         # Airflow Connection ID (MinIO/S3)
        target_table='KKBOX_MEMBERS',         # [주의] Oracle에 타겟 테이블이 존재해야 합니다
        bucket_name='bronze',               # 읽어올 버킷 이름
        from_date=None,                     # Full Load를 위해 None으로 설정 ('YYYYMMDD' 형식 지정 시 Incremental Load)
        to_date=None,
        key_prefix='kkbox-churn-prediction-challenge/members_v3',  # 파일 경로 탐색 규칙 (train.csv 혹은 train_full.csv 탐색)
        file_extension='csv',               # 파일 확장자
        csv_delimiter=',',                  # CSV 구분자
        csv_has_header=True,                # 헤더 존재 여부
        batch_size=50000                    # 한 번에 INSERT 할 배치 사이즈
    )

    load_train_csv 
