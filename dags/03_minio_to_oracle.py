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
    'execution_timeout': timedelta(hours=5) 
}

# DB 연결을 돕는 함수 (재연결을 위해 분리)
def get_oracle_conn(conn_info):
    service_name = conn_info.schema if conn_info.schema else 'Oracle23ai'
    dsn = f"{conn_info.host}:{conn_info.port}/{service_name}"
    conn = oracledb.connect(
        user=conn_info.login,
        password=conn_info.password,
        dsn=dsn
    )
    return conn

def load_parquet_to_oracle(**kwargs):
    # ---------------------------------------------------------
    # 1. 초기 연결 정보 준비
    # ---------------------------------------------------------
    oracle_hook = OracleHook(oracle_conn_id='oracle_conn')
    conn_info = oracle_hook.get_connection('oracle_conn')
    
    # 최초 연결
    conn = get_oracle_conn(conn_info)
    cursor = conn.cursor()
    print("1. Oracle 최초 연결 성공")

    # ---------------------------------------------------------
    # 2. MinIO 스트리밍 준비
    # ---------------------------------------------------------
    year = '2023'
    month = '01'
    bucket_name = 'bronze'
    file_key = f'taxi/year={year}/month={month}/yellow_tripdata_{year}-{month}.parquet'
    
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    file_obj = s3_hook.get_key(key=file_key, bucket_name=bucket_name)
    
    if not file_obj:
        raise Exception("파일이 없습니다!")

    data_stream = io.BytesIO(file_obj.get()['Body'].read())
    parquet_file = pq.ParquetFile(data_stream)
    
    # ---------------------------------------------------------
    # 3. 스트리밍 적재 (재연결 전략)
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

    BATCH_SIZE = 2000     # 2000건씩 적재
    RECONNECT_SIZE = 50000 # 5만 건마다 DB 재연결 (메모리 초기화)
    total_count = 0
    
    print(f"3. 적재 시작 (Batch: {BATCH_SIZE}, Reconnect: {RECONNECT_SIZE})")

    for i, batch in enumerate(parquet_file.iter_batches(batch_size=BATCH_SIZE)):
        try:
            # 1. Pandas 변환
            df_chunk = batch.to_pandas()
            
            # 2. 전처리
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
            
            # 3. DB 적재
            cursor.executemany(insert_sql, rows)
            conn.commit()
            
            total_count += len(rows)
            
            # 4. 메모리 청소
            del df_chunk
            del rows
            gc.collect()

            # [핵심 전략] 5만 건마다 DB 연결 끊고 다시 맺기
            if total_count % RECONNECT_SIZE == 0:
                print(f"   🔄 [Clean-up] {total_count}건 달성. DB 세션 초기화 중...")
                cursor.close()
                conn.close()
                time.sleep(1) # 1초간 완벽하게 연결 해제 대기
                
                # 다시 연결
                conn = get_oracle_conn(conn_info)
                cursor = conn.cursor()
                print(f"   ✅ [Resumed] DB 재연결 완료.")
            
            # 로그는 10만 건마다 한 번만 출력 (로그 부담 최소화)
            elif total_count % 100000 == 0:
                 print(f"   -> 누적 {total_count} 건 적재 진행 중...")

            # 0.01초 휴식 (재연결 전략이 있으므로 휴식은 짧게)
            time.sleep(0.01)

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