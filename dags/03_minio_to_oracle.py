from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
import pendulum
import io
import oracledb

# 1. 설정
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False
}

def load_parquet_to_oracle(**kwargs):
    # 날짜 파라미터 받기 (예: 2023-01)
    year = '2023'
    month = '01'
    
    bucket_name = 'bronze'
    file_key = f'taxi/year={year}/month={month}/yellow_tripdata_{year}-{month}.parquet'
    
    print(f"1. MinIO에서 데이터 읽기 시작: {file_key}")
    
    # S3Hook으로 파일 다운로드 (메모리)
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
    
    if not file_obj:
        raise Exception("파일이 없습니다!")

    # BytesIO를 통해 Pandas로 읽기
    data_stream = io.BytesIO(file_obj.get()['Body'].read())
    df = pd.read_parquet(data_stream)
    
    print(f"2. 데이터 로드 완료. 행 개수: {len(df)}")
    
    # --- 데이터 전처리 ---
    df = df.rename(columns={
        'VendorID': 'VENDOR_ID',
        'tpep_pickup_datetime': 'TPEP_PICKUP_DATETIME',
        'tpep_dropoff_datetime': 'TPEP_DROPOFF_DATETIME',
        'passenger_count': 'PASSENGER_COUNT',
        'trip_distance': 'TRIP_DISTANCE',
        'RatecodeID': 'RATE_CODE_ID',
        'store_and_fwd_flag': 'STORE_AND_FWD_FLAG',
        'PULocationID': 'PULOCATION_ID',
        'DOLocationID': 'DOLOCATION_ID',
        'payment_type': 'PAYMENT_TYPE',
        'fare_amount': 'FARE_AMOUNT',
        'extra': 'EXTRA',
        'mta_tax': 'MTA_TAX',
        'tip_amount': 'TIP_AMOUNT',
        'tolls_amount': 'TOLLS_AMOUNT',
        'improvement_surcharge': 'IMPROVEMENT_SURCHARGE',
        'total_amount': 'TOTAL_AMOUNT',
        'congestion_surcharge': 'CONGESTION_SURCHARGE',
        'airport_fee': 'AIRPORT_FEE'
    })
    
    target_columns = [
        'VENDOR_ID', 'TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME', 
        'PASSENGER_COUNT', 'TRIP_DISTANCE', 'RATE_CODE_ID', 
        'STORE_AND_FWD_FLAG', 'PULOCATION_ID', 'DOLOCATION_ID', 
        'PAYMENT_TYPE', 'FARE_AMOUNT', 'EXTRA', 'MTA_TAX', 
        'TIP_AMOUNT', 'TOLLS_AMOUNT', 'IMPROVEMENT_SURCHARGE', 
        'TOTAL_AMOUNT', 'CONGESTION_SURCHARGE', 'AIRPORT_FEE'
    ]
    
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    df = df[target_columns]
    df = df.fillna(0)

    print("3. Oracle 적재 시작")
    
    # --- [수정된 연결 로직] ---
    oracle_hook = OracleHook(oracle_conn_id='oracle_conn')
    conn_info = oracle_hook.get_connection('oracle_conn')
    
    # Service Name 결정 (UI Schema 필드에 있으면 그거 쓰고, 없으면 Oracle23ai)
    service_name = conn_info.schema if conn_info.schema else 'Oracle23ai'
    
    # DSN 수동 생성 (IP:Port/Service_Name) -> 이러면 TNS 설정을 안 찾습니다.
    dsn = f"{conn_info.host}:{conn_info.port}/{service_name}"
    
    print(f"   -> 접속 시도: {dsn}")

    # oracledb 직접 연결
    conn = oracledb.connect(
        user=conn_info.login,
        password=conn_info.password,
        dsn=dsn
    )
    cursor = conn.cursor()
    
    rows = [tuple(x) for x in df.to_numpy()]
    
    insert_sql = f"""
    INSERT INTO TAXI_DATA ({', '.join(target_columns)}) 
    VALUES ({', '.join([':' + str(i+1) for i in range(len(target_columns))])})
    """
    
    batch_size = 5000
    total_rows = len(rows)
    
    for i in range(0, total_rows, batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(insert_sql, batch)
        conn.commit()
        print(f"   -> {i + len(batch)} / {total_rows} 건 적재 완료")

    cursor.close()
    conn.close()
    print("✅ Oracle 적재 완료!")

with DAG(
    dag_id='03_minio_to_oracle',
    default_args=default_args,
    schedule=None,
    tags=['portfolio', 'oracle', 'elt'],
) as dag:

    load_task = PythonOperator(
        task_id='load_to_oracle',
        python_callable=load_parquet_to_oracle
    )