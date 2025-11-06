from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator  
from common.common_func import get_sftp

from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_python_import_func",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 2, tz="Asia/Seoul"),
    catchup=False
) as dag:
    task_get_sftp = PythonOperator(
        task_id="task_get_sftp",
        python_callable=get_sftp
    )