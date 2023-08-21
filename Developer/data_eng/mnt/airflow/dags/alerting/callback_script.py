
import sys
sys.path.append('/opt/airflow/dags/')

from alerting.slack_alert import task_fail_slack_alert
# The callback function

def callback_function(context):
	"Custom Call back function to have alerts in slack"
	task_fail_slack_alert(context=context)