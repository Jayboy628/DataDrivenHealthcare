import boto3

def copy_object_in_s3(source_bucket, source_key, dest_bucket, dest_key):
    s3 = boto3.client('s3')
    
    try:
        s3.copy_object(Bucket=dest_bucket, 
                       CopySource={'Bucket': source_bucket, 'Key': source_key}, 
                       Key=dest_key)
        print(f"Successfully copied {source_key} to {dest_key}")
    except Exception as e:
        print(f"Error copying object: {e}")

if __name__ == '__main__':
    source_bucket_name = 'snowflake-emr'
    destination_bucket_name = 'snowflake-emr'
    
    # Define the source and destination keys
    source_key_test_load = 'raw_files/TEST_LOAD.csv'
    dest_key_test_load = 'error_files/TEST_LOAD.csv'
    
    source_key_customer = 'raw_files/customer.csv'
    dest_key_customer = 'error_files/customer.csv'
    
    # Attempt to copy TEST_LOAD.csv
    copy_object_in_s3(source_bucket_name, source_key_test_load, destination_bucket_name, dest_key_test_load)

    # Attempt to copy customer.csv
    copy_object_in_s3(source_bucket_name, source_key_customer, destination_bucket_name, dest_key_customer)

