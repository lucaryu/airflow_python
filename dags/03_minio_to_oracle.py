from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
import pendulum
import io
import oracledb
import pyarrow.parquet as pq  # [추가] 대용량 파일 끊어 읽기용

# 1. 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False
}

def load_parquet_to_oracle(**kwargs):
    # ---------------------------------------------------------
    # 1. Oracle 연결 (먼저 연결을 맺어둠)
    # ---------------------------------------------------------
    print("1. Oracle 연결 준비")
    oracle_hook = OracleHook(oracle_conn_id='oracle_conn')
    conn_info = oracle_hook.get_connection('oracle_conn')
    
    # Service Name 및 DSN 설정
    service_name = conn_info.schema if conn_info.schema else 'Oracle23ai'
    dsn = f"{conn_info.host}:{conn_info.port}/{service_name}"
    
    conn = oracledb.connect(
        user=conn_info.login,
        password=conn_info.password,
        dsn=dsn
    )
    cursor = conn.cursor()
    print(f"   -> Oracle 접속 성공: {dsn}")

    # ---------------------------------------------------------
    # 2. MinIO에서 파일 스트림 가져오기
    # ---------------------------------------------------------
    year = '2023'
    month = '01'
    bucket_name = 'bronze'
    file_key = f'taxi/year={year}/month={month}/yellow_tripdata_{year}-{month}.parquet'
    
    print(f"2. MinIO 파일 열기 (Streaming): {file_key}")
    
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
    
    if not file_obj:
        raise Exception("파일이 없습니다!")

    # 파일을 통째로 읽지 않고, 스트림(빨대)만 꽂아둡니다.
    data_stream = io.BytesIO(file_obj.get()['Body'].read())
    
    # PyArrow로 Parquet 파일 열기
    parquet_file = pq.ParquetFile(data_stream)
    
    # ---------------------------------------------------------
    # 3. 배치 단위로 읽어서 -> 전처리하고 -> 바로 적재 (메모리 절약 끝판왕)
    # ---------------------------------------------------------
    
    # Oracle 테이블 컬럼 순서 정의
    target_columns = [
        'VENDOR_ID', 'TPEP_PICKUP_DATETIME', 'TPEP_DROPOFF_DATETIME', 
        'PASSENGER_COUNT', 'TRIP_DISTANCE', 'RATE_CODE_ID', 
        'STORE_AND_FWD_FLAG', 'PULOCATION_ID', 'DOLOCATION_ID', 
        'PAYMENT_TYPE', 'FARE_AMOUNT', 'EXTRA', 'MTA_TAX', 
        'TIP_AMOUNT', 'TOLLS_AMOUNT', 'IMPROVEMENT_SURCHARGE', 
        'TOTAL_AMOUNT', 'CONGESTION_SURCHARGE', 'AIRPORT_FEE'
    ]
    
    insert_sql = f"""
    INSERT INTO TAXI_DATA ({', '.join(target_columns)}) 
    VALUES ({', '.join([':' + str(i+1) for i in range(len(target_columns))])})
    """

    BATCH_SIZE = 10000  # 한 번에 처리할 행 개수
    total_count = 0
    
    print(f"3. 스트리밍 적재 시작 (Batch Size: {BATCH_SIZE})")

    # [핵심] iter_batches를 쓰면 파일의 일부분만 메모리에 올립니다.
    for i, batch in enumerate(parquet_file.iter_batches(batch_size=BATCH_SIZE)):
        
        # 1) PyArrow Batch -> Pandas DataFrame 변환 (작은 조각만 변환)
        df_chunk = batch.to_pandas()
        
        # 2) 전처리 (컬럼명 변경)
        df_chunk = df_chunk.rename(columns={
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
        
        # 3) 없는 컬럼 채우기 & 결측치 처리
        for col in target_columns:
            if col not in df_chunk.columns:
                df_chunk[col] = None
        
        df_chunk = df_chunk[target_columns].fillna(0)
        
        # 4) 데이터 준비
        rows = [tuple(x) for x in df_chunk.to_numpy()]
        
        # 5) DB 적재
        try:
            cursor.executemany(insert_sql, rows)
            conn.commit()
            total_count += len(rows)
            print(f"   -> [Chunk {i+1}] 누적 {total_count} 건 적재 완료")
        except Exception as e:
            print(f"   -> [Chunk {i+1}] 에러 발생: {e}")
            raise e
            
        # 루프가 끝나면 df_chunk는 메모리에서 사라집니다. (메모리 확보)

    cursor.close()
    conn.close()
    print(f"✅ 총 {total_count} 건 적재 완료!")

with DAG(
    dag_id='03_minio_to_oracle',
    default_args=default_args,
    schedule=None,
    tags=['portfolio', 'oracle', 'elt', 'streaming'],
) as dag:

    load_task = PythonOperator(
        task_id='load_to_oracle',
        python_callable=load_parquet_to_oracle
    )