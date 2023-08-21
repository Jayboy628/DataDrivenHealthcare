import boto3

# ... Existing Code ...

SNOWFLAKE_TABLES = ['users', 'location', 'transactiontype', 'code', 'date', 'transactiondetails', 'payer']


# Initialize SSM Client
ssm = boto3.client('ssm', region_name='us-east-1') # Replace 'us-east-1' with your AWS region if different


def get_ssm_parameter(param_name, default=None):
    try:
        return ssm.get_parameter(Name=param_name, WithDecryption=True)['Parameter']['Value']
    except Exception as e:
        print(f"Error fetching parameter {param_name}: {str(e)}")
        return default

SNOWFLAKE_CONFIG = {
    'account': get_ssm_parameter('/snowflake/account'),
    'user': get_ssm_parameter('/snowflake/username'),
    'password': get_ssm_parameter('/snowflake/password'),
    'warehouse': get_ssm_parameter('/snowflake/warehouse'),
    'database': get_ssm_parameter('/snowflake/database'),
    'schema': get_ssm_parameter('/snowflake/schema')
}

SCHEMA_FILE_MAPPING = {
    'CHART': ['code'],
    'BILLING': ['transactiondetails', 'transactiontype', 'payer'],
    'REGISTER': ['users', 'location', 'date']
}

