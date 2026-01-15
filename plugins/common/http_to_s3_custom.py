from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults
import requests
import pendulum
import io

class YearMonthRangeToS3Operator(BaseOperator):
    """
    지정된 날짜 범위(from_date ~ to_date)를 월 단위로 순회하며
    HTTP URL에서 파일을 다운로드하여 S3(MinIO)에 업로드하는 오퍼레이터
    """
    
    # Airflow UI나 DAG에서 Jinja Template({{ }})을 쓸 수 있게 허용할 필드들
    template_fields = ('from_date', 'to_date', 'base_url', 's3_key')

    @apply_defaults
    def __init__(
        self,
        aws_conn_id,
        bucket_name,
        base_url,
        s3_key,
        from_date,
        to_date,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.base_url = base_url   # 예: .../yellow_tripdata_{year}-{month}.parquet
        self.s3_key = s3_key       # 예: taxi/year={year}/month={month}/...
        self.from_date = from_date
        self.to_date = to_date

    def execute(self, context):
        # 날짜 파싱
        start_dt = pendulum.from_format(str(self.from_date), 'YYYYMMDD')
        end_dt = pendulum.from_format(str(self.to_date), 'YYYYMMDD')
        
        self.log.info(f"기간 처리 시작: {start_dt.to_date_string()} ~ {end_dt.to_date_string()}")
        
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        
        # 버킷 확인 및 생성
        if not s3_hook.check_for_bucket(self.bucket_name):
            s3_hook.create_bucket(self.bucket_name)

        current_date = start_dt
        processed_months = set()

        while current_date <= end_dt:
            year = current_date.format('YYYY')
            month = current_date.format('MM')
            key_ym = f"{year}-{month}"

            if key_ym in processed_months:
                current_date = current_date.add(months=1)
                continue
            
            processed_months.add(key_ym)

            # URL과 S3 경로의 {year}, {month} 부분을 실제 값으로 치환
            target_url = self.base_url.format(year=year, month=month)
            target_key = self.s3_key.format(year=year, month=month)

            self.log.info(f"[{year}-{month}] 다운로드: {target_url}")

            try:
                response = requests.get(target_url, stream=True)
                if response.status_code == 404:
                    self.log.warning(f"데이터 없음 (404): {target_url}")
                else:
                    response.raise_for_status()
                    file_obj = io.BytesIO(response.content)
                    
                    s3_hook.load_file_obj(
                        file_obj=file_obj,
                        key=target_key,
                        bucket_name=self.bucket_name,
                        replace=True
                    )
                    self.log.info(f"업로드 완료: s3://{self.bucket_name}/{target_key}")

            except Exception as e:
                self.log.error(f"에러 발생: {e}")
                raise e
            
            # 다음 달로 이동
            current_date = current_date.add(months=1)