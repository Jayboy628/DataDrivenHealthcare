import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys

default_args = {
    'owner': 'me',
    'start_date': airflow.utils.dates.days_ago(0),
}

dag = DAG(
    'python_version_dag',
    default_args=default_args,
    description='A simple DAG to print Python version',
    schedule_interval='@once',
)

def print_python_version():
    print("Python version:", sys.version)
    print("\nPython sys.path:", sys.path)

print_version_op = PythonOperator(
    task_id='print_python_version',
    python_callable=print_python_version,
    dag=dag,
)
