import sys
sys.path.append('/opt/airflow/dags/')

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

from datetime import datetime, timedelta

import boto3

from source_load.data_load import load_data  # Updated import statement

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def load_to_snowflake(*args, **kwargs):
    print("Starting data load to Snowflake...")
    load_data()
    print("Finished data load to Snowflake.")


def check_for_files(bucket_name, prefix):
    """Check for files in an S3 bucket path (prefix)"""
    
    s3 = boto3.client('s3')
    
    # List objects within the given prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    print(f"Listing objects for bucket {bucket_name} with prefix {prefix}: {response}")  # Add logging
    
    # Check if any files are found
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Size'] > 0:
                print(f"Found file {obj['Key']} of size {obj['Size']}")  # Add logging
                return True
    return False


def branch_based_on_files_existence(*args, **kwargs):
    bucket_name = kwargs['bucket_name']
    prefix = kwargs['prefix']
    exists = check_for_files(bucket_name, prefix)
    if exists:
        return 'load_to_snowflake'
    else:
        return 'end_task'

with DAG(
    "DataDriven_Healthcare",
    default_args=default_args,
    start_date=datetime(2023, 5, 12),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(task_id='end_task')


    s3_list = S3ListOperator(
        task_id='list_files_in_raw_files',
        bucket='snowflake-emr',
        prefix='raw_files/',
        aws_conn_id='aws_default'
    )

    branch = BranchPythonOperator(
        task_id='branch_check_raw_files',
        python_callable=branch_based_on_files_existence,
        provide_context=True,
        op_args=[],
        op_kwargs={'bucket_name': 'snowflake-emr', 'prefix': 'raw_files/'}
    )

    load_data_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True
    )

    start_task >> s3_list >> branch
    branch >> load_data_task >> end_task
    branch >> end_task
