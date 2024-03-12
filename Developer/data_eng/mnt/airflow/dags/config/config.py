import boto3


# Tables configuration
SNOWFLAKE_TABLES = ['USERS', 'location', 'transactiontype', 'code', 'date', 'transactiondetails', 'payer']

# Initialize AWS SSM Client
ssm = boto3.client('ssm', region_name='us-east-1')  # Adjust the AWS region if different

# Fetch parameter from SSM
def get_ssm_parameter(param_name, default=None):
    try:
        return ssm.get_parameter(Name=param_name, WithDecryption=True)['Parameter']['Value']
    except Exception as e:
        raise RuntimeError(f"Error fetching parameter {param_name}: {str(e)}") # Slack Configurations
SLACK_WEBHOOK_URL = get_ssm_parameter('/airflow/SlackWebhookURL')
SLACK_CONFIG = {
    'token': get_ssm_parameter('/airflow/slack_token')
}

# Snowflake Configurations
SNOWFLAKE_CONFIG = {
    'account': get_ssm_parameter('/snowflake/account'),
    'user': get_ssm_parameter('/snowflake/user_raw/username'),
    'password': get_ssm_parameter('/snowflake/user_raw/password'),
    'warehouse': get_ssm_parameter('/snowflake/warehouse'), 
    'database': get_ssm_parameter('/snowflake/database'),
    'database_raw': get_ssm_parameter('/snowflake/database_raw'),
    'schema': get_ssm_parameter('/snowflake/schema'),
    'role': 'DEVELOPER'  # or 'ROLE_RAW' if you created a new role
}


# Schema File Mapping Configurations
SCHEMA_FILE_MAPPING = {
    'CHART': ['CODE'],
    'BILLING': ['transactiondetails', 'transactiontype', 'payer'],
    'REGISTER': ['USERS', 'location', 'date']
}

# Utility functions to work with SCHEMA_FILE_MAPPING

def get_tables_for_schema(schema_name):
    """Return a list of tables for a given schema."""
    return SCHEMA_FILE_MAPPING.get(schema_name, [])

def get_all_tables():
    """Return a list of all tables across all schemas."""
    return [table for tables_list in SCHEMA_FILE_MAPPING.values() for table in tables_list]
