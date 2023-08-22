import boto3

def key_exists(bucket, key):
    s3 = boto3.client('s3')
    
    try:
        s3.head_object(Bucket=bucket, Key=key)
        print(f"'{key}' exists in '{bucket}'.")
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"'{key}' does not exist in '{bucket}'.")
        else:
            print(f"An error occurred: {e}")
        return False

# Test
bucket_name = 'snowflake-emr'
source_key = 'raw_files/TEST_LOAD.csv'
key_exists(bucket_name, source_key)
