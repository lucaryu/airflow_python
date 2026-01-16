from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
import pendulum
import io
import oracledb
import pyarrow.parquet as pq
import gc
import time
from datetime import timedelta

# 1. 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    'catchup': False,
    # 넉넉하게 5시간 설정 (안전빵)
    'execution_timeout': timedelta(hours=5) 
}

def load_parquet_to_oracle(**kwargs):
    print("1. Oracle 연결 준비")
    oracle_hook = OracleHook(oracle_conn_id='oracle_conn')
    conn_info = oracle_hook.get_connection('oracle_conn')
    
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
    # 2. MinIO 스트리밍 준비
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

    data_stream = io.BytesIO(file_obj.get()['Body'].read())
    parquet_file = pq.ParquetFile(data_stream)
    
    # ---------------------------------------------------------
    # 3. 스트리밍 적재
    # ---------------------------------------------------------
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

    # [수정] 3000 -> 1000 (Oracle Free 버전이 소화하기 편한 크기)
    BATCH_SIZE = 1000 
    total_count = 0
    
    # [수정] 0.05초 -> 0.1초 (Oracle DB가 디스크에 쓸 시간을 충분히 줌)
    SLEEP_TIME = 0.1
    
    print(f"3. 스트리밍 적재 시작 (Batch: {BATCH_SIZE}, Sleep: {SLEEP_TIME}s)")

    for i, batch in enumerate(parquet_file.iter_batches(batch_size=BATCH_SIZE)):
        try:
            df_chunk = batch.to_pandas()
            
            # 전처리
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
            
            for col in target_columns:
                if col not in df_chunk.columns:
                    df_chunk[col] = None
            
            df_chunk = df_chunk[target_columns].fillna(0)
            rows = [tuple(x) for x in df_chunk.to_numpy()]
            
            cursor.executemany(insert_sql, rows)
            conn.commit()
            
            total_count += len(rows)
            
            # 로그는 10,000건마다 출력
            if (i + 1) % 10 == 0: 
                print(f"   -> [Chunk {i+1}] 누적 {total_count} 건 적재 완료")

            # 메모리 청소
            del df_chunk
            del rows
            gc.collect() 
            
            # 휴식
            time.sleep(SLEEP_TIME)

        except Exception as e:
            print(f"   -> [Chunk {i+1}] 에러 발생: {e}")
            raise e

    cursor.close()
    conn.close()
    print(f"✅ 총 {total_count} 건 적재 완료!")

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