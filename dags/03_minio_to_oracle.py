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
    # 테스트를 위해 고정값 사용하거나 logical_date 사용 가능
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

    # BytesIO를 통해 Pandas로 읽기 (pyarrow 엔진 필요)
    data_stream = io.BytesIO(file_obj.get()['Body'].read())
    df = pd.read_parquet(data_stream)
    
    print(f"2. 데이터 로드 완료. 행 개수: {len(df)}")
    
    # --- 데이터 전처리 (Oracle 컬럼명과 타입에 맞추기) ---
    # 컬럼명 매핑 (Parquet -> Oracle)
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
    
    # 필요한 컬럼만 선택 & 대문자 변환
    target_columns = [
        'VENDOR_ID', 'TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME', 
        'PASSENGER_COUNT', 'TRIP_DISTANCE', 'RATE_CODE_ID', 
        'STORE_AND_FWD_FLAG', 'PULOCATION_ID', 'DOLOCATION_ID', 
        'PAYMENT_TYPE', 'FARE_AMOUNT', 'EXTRA', 'MTA_TAX', 
        'TIP_AMOUNT', 'TOLLS_AMOUNT', 'IMPROVEMENT_SURCHARGE', 
        'TOTAL_AMOUNT', 'CONGESTION_SURCHARGE', 'AIRPORT_FEE'
    ]
    
    # 없는 컬럼은 0으로 채우거나 제외 (안전장치)
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    df = df[target_columns] # 순서 맞추기
    
    # NaN(결측치) 처리 -> Oracle에서는 NULL로 들어가게 처리하거나 0으로 치환
    df = df.fillna(0)

    print("3. Oracle 적재 시작")
    
    # OracleHook 사용
    oracle_hook = OracleHook(oracle_conn_id='oracle_conn')
    
    # oracledb의 thin 모드 사용 (Instant Client 없이 접속)
    # Airflow Connection 정보를 가져와서 직접 연결 생성
    conn = oracle_hook.get_conn()
    cursor = conn.cursor()
    
    # 배치 처리를 위한 데이터 변환 (DataFrame -> List of Tuples)
    # 성능을 위해 1000건씩 끊어서 넣거나 executemany 사용
    rows = [tuple(x) for x in df.to_numpy()]
    
    insert_sql = f"""
    INSERT INTO TAXI_DATA ({', '.join(target_columns)}) 
    VALUES ({', '.join([':' + str(i+1) for i in range(len(target_columns))])})
    """
    
    # 대량 데이터(Batch) 입력
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