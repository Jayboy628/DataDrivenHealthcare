# s3_utils.py

import boto3
import logging
from airflow.hooks.S3_hook import S3Hook
from alerting.slack_alert import send_slack_notification

# Setting up logging
log = logging.getLogger(__name__)

# Initialize boto3 S3 resource
s3 = boto3.resource('s3')

def list_s3_files(bucket_name, prefix='', aws_conn_id='default'):
    """
    List files in an S3 bucket with optional prefix filtering.

    :param bucket_name: Name of the S3 bucket.
    :param prefix: Prefix to filter the S3 objects.
    :param aws_conn_id: AWS connection ID for Airflow. Uses boto3 if 'default'.
    :return: List of file paths.
    """
    try:
        if aws_conn_id == 'default':
            # Using boto3 for S3 access
            bucket = s3.Bucket(bucket_name)
            files = [obj.key for obj in bucket.objects.filter(Prefix=prefix)]
        else:
            # Using Airflow's S3Hook for S3 access
            hook = S3Hook(aws_conn_id)
            files = hook.list_keys(bucket_name, prefix=prefix)
        return files
    except Exception as e:
        log.error(f"Error listing files in S3: {str(e)}")
        send_slack_notification(f"Error listing files in S3: {str(e)}", message_type='error')
        raise

def check_for_files(bucket_name, prefix, **kwargs):
    """
    Check for files in an S3 bucket and send a Slack notification based on the result.

    :param bucket_name: Name of the S3 bucket.
    :param prefix: Prefix to filter the S3 objects.
    :return: Boolean indicating if files exist.
    """
    try:
        files = list_s3_files(bucket_name, prefix)
        files_exist = len(files) > 0

        if files_exist:
            send_slack_notification("Files found in S3 bucket.", message_type='success')
        else:
            send_slack_notification("No files found in S3 bucket.", message_type='failure')

        return files_exist
    except Exception as e:
        send_slack_notification(f"Error checking files in S3: {str(e)}", message_type='error')
        raise

def branch_based_on_files_existence(*args, **kwargs):
    """
    Branch workflow based on the existence of files in S3.

    :param args: Positional arguments.
    :param kwargs: Keyword arguments.
    :return: Branching decision.
    """
    try:
        bucket_name = kwargs.get('bucket_name')
        prefix = kwargs.get('prefix', '')

        files_exist = check_for_files(bucket_name, prefix)
        return 'path_a' if files_exist else 'path_b'
    except Exception as e:
        log.error(f"Error in branching logic: {str(e)}")
        raise
