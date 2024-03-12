from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 9, 17),
}

dag = DAG(
    'snowflake_test_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
)

snowflake_task = SnowflakeOperator(
    task_id='snowflake_task',
    sql="SELECT current_version();",
    snowflake_conn_id="snowflake_default",
    dag=dag
)

snowflake_task

