from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
import pendulum
import io
import oracledb

# 1. 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False
}

def load_parquet_to_oracle(**kwargs):
    # ---------------------------------------------------------
    # 1. 데이터 다운로드 (MinIO)
    # ---------------------------------------------------------
    year = '2023'
    month = '01'
    bucket_name = 'bronze'
    file_key = f'taxi/year={year}/month={month}/yellow_tripdata_{year}-{month}.parquet'
    
    print(f"1. MinIO에서 데이터 읽기 시작: {file_key}")
    
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
    
    if not file_obj:
        raise Exception("파일이 없습니다!")

    # Parquet 읽기
    data_stream = io.BytesIO(file_obj.get()['Body'].read())
    df = pd.read_parquet(data_stream)
    
    print(f"2. 데이터 로드 완료. 행 개수: {len(df)}")
    
    # ---------------------------------------------------------
    # 2. 데이터 전처리 (컬럼 매핑 & 결측치 처리)
    # ---------------------------------------------------------
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
    
    # Oracle 테이블 순서와 맞추기
    target_columns = [
        'VENDOR_ID', 'TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME', 
        'PASSENGER_COUNT', 'TRIP_DISTANCE', 'RATE_CODE_ID', 
        'STORE_AND_FWD_FLAG', 'PULOCATION_ID', 'DOLOCATION_ID', 
        'PAYMENT_TYPE', 'FARE_AMOUNT', 'EXTRA', 'MTA_TAX', 
        'TIP_AMOUNT', 'TOLLS_AMOUNT', 'IMPROVEMENT_SURCHARGE', 
        'TOTAL_AMOUNT', 'CONGESTION_SURCHARGE', 'AIRPORT_FEE'
    ]
    
    # 없는 컬럼은 None으로 채우기
    for col in target_columns:
        if col not in df.columns:
            df[col] = None

    df = df[target_columns]
    
    # NaN(결측치)를 0이나 빈 값으로 채우기 (Oracle 에러 방지)
    df = df.fillna(0)

    # ---------------------------------------------------------
    # 3. Oracle 적재 (메모리 절약형 Chunk Processing)
    # ---------------------------------------------------------
    print("3. Oracle 연결 시도")
    
    # Airflow Connection 정보 가져오기
    oracle_hook = OracleHook(oracle_conn_id='oracle_conn')
    conn_info = oracle_hook.get_connection('oracle_conn')
    
    # Service Name 설정 (UI Schema에 값이 없으면 기본값 Oracle23ai 사용)
    service_name = conn_info.schema if conn_info.schema else 'Oracle23ai'
    
    # DSN 수동 생성 (TNS 에러 방지용)
    dsn = f"{conn_info.host}:{conn_info.port}/{service_name}"
    print(f"   -> 접속 정보: {dsn}")

    # oracledb 직접 연결
    conn = oracledb.connect(
        user=conn_info.login,
        password=conn_info.password,
        dsn=dsn
    )
    cursor = conn.cursor()

    # 쿼리 준비
    insert_sql = f"""
    INSERT INTO TAXI_DATA ({', '.join(target_columns)}) 
    VALUES ({', '.join([':' + str(i+1) for i in range(len(target_columns))])})
    """
    
    # [핵심] 메모리 절약을 위한 제너레이터 사용
    # df.to_numpy()로 전체 변환하지 않고, 한 줄씩 꺼내 씁니다.
    data_generator = df.itertuples(index=False, name=None)
    
    batch_size = 5000  # 5000개씩 끊어서 적재
    batch = []
    total_count = 0
    
    print(f"4. 적재 시작 (배치 사이즈: {batch_size})")

    for row in data_generator:
        batch.append(row)
        
        # 배치가 꽉 차면 DB에 밀어넣고 메모리 비움
        if len(batch) >= batch_size:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            total_count += len(batch)
            print(f"   -> {total_count} 건 적재 완료...")
            batch = []  # 메모리 초기화
            
    # 남은 데이터 처리
    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        total_count += len(batch)
        print(f"   -> {total_count} 건 적재 완료 (최종)")

    cursor.close()
    conn.close()
    print("✅ Oracle 적재가 성공적으로 완료되었습니다!")

with DAG(
    dag_id='03_minio_to_oracle',
    default_args=default_args,
    schedule=None,
    tags=['portfolio', 'oracle', 'elt', 'optimized'],
) as dag:

    load_task = PythonOperator(
        task_id='load_to_oracle',
        python_callable=load_parquet_to_oracle
    )