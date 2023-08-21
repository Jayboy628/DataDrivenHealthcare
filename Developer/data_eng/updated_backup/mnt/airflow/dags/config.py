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
# S3_CONFIG = {
#     'aws_key_id': get_ssm_parameter('/s3/aws_key_id'),
#     'aws_secret_key': get_ssm_parameter('/s3/aws_secret_key')
# }
