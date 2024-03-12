import logging
import os
import re
import sys
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Adjusting sys.path for custom modules
sys.path.insert(0, "/opt/airflow/dags/")

# Importing custom modules
from callbacks.callback_script import start_dag_notification
from alerting.slack_alert import slack_after_load, slack_before_load, start_slack_alert,task_status_slack_alert
from scripts.s3_utils import branch_based_on_files_existence, list_files_in_stage, list_s3_files
from scripts.load_data_to_snowflake import airflow_task_load_data
from scripts.notifications import notify_on_failure, task_success_slack_alert

# Configuring logging
log = logging.getLogger(__name__)
log.info("This is an info message.")

BUCKET_NAME = Variable.get("S3_BUCKET_NAME", default_var="snowflake-emr")
PREFIX = Variable.get("S3_PREFIX", default_var="raw_files/")
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0)
}

with DAG(
    "DataDriven_Healthcare",
    default_args=default_args,
    start_date=datetime(2023, 5, 12),
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=start_slack_alert
    )

    notify_slack_on_start = PythonOperator(
        task_id='notify_slack_on_start',
        python_callable=start_dag_notification
    )

    list_files_in_stage_task = PythonOperator(
        task_id='list_files_in_stage',
        python_callable=list_files_in_stage,
        on_failure_callback=notify_on_failure
    )

    AWS_CONN_ID = 'aws_default'  # Define AWS_CONN_ID here

    list_files_task = PythonOperator(
        task_id='list_files_in_raw_files',
        python_callable=list_s3_files,
        op_args=[BUCKET_NAME, PREFIX, AWS_CONN_ID]
    )

    branch_check_raw_files = PythonOperator(
        task_id='branch_check_raw_files',
        python_callable=branch_based_on_files_existence,
        op_args=[BUCKET_NAME, PREFIX],
        op_kwargs={'bucket_name': BUCKET_NAME, 'prefix': PREFIX}
    )

    slack_before_load_task = PythonOperator(
        task_id='slack_before_load',
        python_callable=slack_before_load
    )

    load_to_snowflake_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=airflow_task_load_data,
        on_success_callback=lambda context: task_status_slack_alert(context, 'success'),
        on_failure_callback=lambda context: task_status_slack_alert(context, 'failure'),
    )

    slack_after_load_task = PythonOperator(
        task_id='slack_after_load',
        python_callable=slack_after_load
    )
    
    run_stage_models = BashOperator(
        task_id='run_stage_models',
        bash_command='...'
    )

    slack_success_alert = PythonOperator(
        task_id='slack_success_alert',
        python_callable=task_success_slack_alert
    )

    end_task = DummyOperator(task_id='end_task')

    # Define the DAG execution sequence

    start_task >> notify_slack_on_start >> list_files_in_stage_task >> list_files_task >> branch_check_raw_files
    branch_check_raw_files >> slack_before_load_task >> load_to_snowflake_task >> slack_after_load_task >> run_stage_models >> slack_success_alert >> end_task

# remember change s3_list to list_files_task
