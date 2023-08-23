


# Imports

# Importing the base book
from airflow.hooks.base_hook import BaseHook
# Importing the Slack Webhook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
# importing the common settings
# Get the System Environment

def task_fail_slack_alert(dag):
    """
    This callback function would be called if any of the tasks fail in airflow DAG
    which would inturn send a message to a slack channel.
    :param context:
    :return:
    """

    # Create the slack connection and get the web token
    slack_web_hook_token = BaseHook.get_connection('Slack_Connection').password

    # prepare the message which needs to be send to slack
    slack_msg = """
            :red_circle: {dag} Workflow Failed at task {task}
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            exec_date=context.get('execution_date')

        )


    # Send the actual message to Slack.
    failed_alert = SlackWebhookOperator(
        task_id='SLACK_FAILED_ALERT',
        http_conn_id='Slack_Connection',
        webhook_token=slack_web_hook_token,
        message=slack_msg)

    # Execute the Slack WebHook Dag



def task_success_slack_alert(dag):
    """
    Send Slack Success messages.
    :param dag: Dag to which this task needs to be attached to.
    :return:
    """

    # Create the slack connection and get the web token
    slack_web_hook_token = BaseHook.get_connection('Slack_Connection').password

    # prepare the message which needs to be send to slack
    slack_msg = """
                :white_check_mark: {{ task_instance.dag_id }} Workflow completed successfully.
                *Task*: {{ task_instance.task_id }} 
                *Dag*: {{ task_instance.dag_id }}
                *Execution Time*: {{ execution_date }} 
                """

    # Send the actual message to Slack.
    task_success_alert = SlackWebhookOperator(
        task_id='SLACK_SUCCESS_ALERT',
        http_conn_id='Slack_Connection',
        webhook_token=slack_web_hook_token,
        on_failure_callback=None,
        message=slack_msg,
        dag=dag)

    # Execute the Slack WebHook Dag
    return task_success_alert
	
	