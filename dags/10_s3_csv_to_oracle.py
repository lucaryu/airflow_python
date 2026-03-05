from airflow import DAG
from airflow.models.param import Param
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
    tags=['s3', 'oracle', 'csv', 'kkbox'],
    params={
        "target_table": Param("members_v3", type="string", description="Oracle 타겟 테이블명"),
        "key_prefix": Param("kkbox-churn-prediction-challenge/members_v3.csv/", type="string", description="S3 파일 경로 탐색 규칙 (Prefix)"),
        "file_extension": Param("csv", enum=["csv", "parquet"], description="파일 확장자 (csv 또는 parquet)"),
        "csv_delimiter": Param(",", type="string", description="CSV 구분자"),
        "csv_has_header": Param(True, type="boolean", description="CSV 헤더 존재 여부 (예: True/False)"),
    }
) as dag:

    # =========================================================================
    # [사용 방법]
    # S3ToOracleOperator를 사용하여 MinIO(S3)의 CSV/Parquet 파일을 Oracle로 적재합니다.
    # Trigger DAG w/ config를 활용하여 테이블명, prefix, 확장자, 구분자를 변경하면서 실행 가능합니다.
    # 해당 타겟 테이블(target_table)은 Oracle DB에 미리 생성되어 있어야 합니다.
    # =========================================================================

    load_csv = S3ToOracleOperator(
        task_id='load_train_csv',
        oracle_conn_id='oracle_conn',       # Airflow Connection ID (Oracle)
        minio_conn_id='minio_conn',         # Airflow Connection ID (MinIO/S3)
        target_table='{{ params.target_table }}',         # 템플릿 처리 (파라미터로 받음)
        bucket_name='bronze',               # 읽어올 버킷 이름
        from_date=None,                     # Full Load를 위해 None으로 설정 ('YYYYMMDD' 형식 지정 시 Incremental Load)
        to_date=None,
        key_prefix='{{ params.key_prefix }}',  # 템플릿 처리 (파라미터로 받음)
        file_extension='{{ params.file_extension }}',     # 템플릿 처리 (파라미터로 받음)
        csv_delimiter='{{ params.csv_delimiter }}',       # 템플릿 처리 (파라미터로 받음)
        csv_has_header='{{ params.csv_has_header }}',     # 템플릿 처리 (파라미터로 받음)
        batch_size=50000                    # 한 번에 INSERT 할 배치 사이즈
    )

    load_csv 