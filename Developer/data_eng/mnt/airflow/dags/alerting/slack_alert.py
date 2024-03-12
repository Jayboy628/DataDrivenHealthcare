import logging
import requests  # Assuming you're using requests to send messages to Slack

# Setup logger
log = logging.getLogger(__name__)

def send_slack_notification(message, context=None, message_type='info'):
    """
    General function to send a Slack message.

    :param message: The message to be sent.
    :param context: Airflow context, if available.
    :param message_type: Type of the message ('info', 'success', 'failure').
    """
    slack_webhook_url = "YOUR_SLACK_WEBHOOK_URL"  # Replace with your Slack webhook URL

    # Customize the message based on the type
    if message_type == 'success':
        message = f":white_check_mark: {message}"
    elif message_type == 'failure':
        message = f":x: {message}"
    else:
        message = f":information_source: {message}"

    payload = {"text": message}

    try:
        response = requests.post(slack_webhook_url, json=payload)
        response.raise_for_status()  # Raise an exception for HTTP errors
    except Exception as e:
        log.error(f"Error sending Slack notification: {e}")
        # Consider additional error handling if necessary

def start_dag_notification(context):
    """
    Send a Slack notification at the start of the DAG execution.

    :param context: Airflow context.
    """
    try:
        dag_id = context.get('dag', {}).get('dag_id', 'Unknown DAG')
        message = f"DAG {dag_id} started."
        send_slack_notification(message, context, message_type='info')
    except KeyError as e:
        log.error(f"Missing key in context: {e}")

def task_status_slack_alert(context, status):
    """
    Send a Slack alert based on task status.

    :param context: Airflow context.
    :param status: Status of the task ('success' or 'failure').
    """
    try:
        task_id = context['task_instance'].task_id
        if status == 'success':
            message = f"Task {task_id} completed successfully."
            send_slack_notification(message, context, message_type='success')
        elif status == 'failure':
            message = f"Task {task_id} failed."
            send_slack_notification(message, context, message_type='failure')
    except KeyError as e:
        log.error(f"Missing key in context: {e}")
