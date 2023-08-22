from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

def print_python_version():
    print(sys.version)

with DAG('python_version_check', start_date=datetime(2023, 8, 6)) as dag:

    print_version = PythonOperator(
        task_id='print_version',
        python_callable=print_python_version,
    )

