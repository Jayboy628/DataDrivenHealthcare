import logging
import snowflake.connector
from config import SNOWFLAKE_CONFIG
import boto3
from datetime import datetime

from config import SNOWFLAKE_CONFIG, SNOWFLAKE_TABLES


def get_processed_files(bucket_name, processed_folder_prefix):
    s3 = boto3.client('s3', region_name='us-east-1')  # specify the region if you haven't already
    processed_files_set = set()

    # Listing objects within the processed prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=processed_folder_prefix)

    if 'Contents' in response:
        for obj in response['Contents']:
            # Extract base name and timestamp
            file_name = obj['Key'].split('/')[-1]  # get just the file name
            base_name, timestamp = file_name.split('_')[0], file_name.split('_')[1][:14]
            processed_files_set.add(f"{base_name}_{timestamp}")

    return processed_files_set


def is_processed(file_name, processed_files):
    base_name, timestamp = file_name.split('_')[0], file_name.split('_')[1][:14]  # split to get base name and timestamp
    return f"{base_name}_{timestamp}" in processed_files


def load_data_from_stage_to_table(cur, file_name):
    try:
        # Extract the table name from the file name
        table_name = file_name.split('_')[0].upper()
        
        # Copy data from external stage to the table in Snowflake
        copy_sql = f"""
        COPY INTO {table_name}
        FROM @HEALTHCARE_RAW.external_stages.aws_stage/{file_name}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"'skip_header=1)
        ON_ERROR = 'CONTINUE'
        """
        cur.execute(copy_sql)
        print(f"Data loaded into {table_name} from {file_name}.")
    except Exception as e:
        print(f"Error during data load from {file_name} to {table_name}: {e}")



def move_file_in_s3(source_bucket, source_key, destination_bucket, destination_key):
    s3 = boto3.client('s3', region_name='us-east-1')  # specify the region if you haven't already
    
    # Copy the file to the new location
    try:
        print(f"Copying from {source_bucket}/{source_key} to {destination_bucket}/{destination_key}")  # Logging
        s3.copy_object(Bucket=destination_bucket, CopySource={'Bucket': source_bucket, 'Key': source_key}, Key=destination_key)
    except Exception as e:
        print(f"Error copying object: {e}")
        return  # Exit the function if copying fails
    
    # Delete the file from the old location
    try:
        s3.delete_object(Bucket=source_bucket, Key=source_key)
        print(f"Moved {source_key} to {destination_key}.")
    except Exception as e:
        print(f"Error deleting the old object: {e}")

def load_data():
    try:
        # Snowflake setup
        con = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG['user'],
            password=SNOWFLAKE_CONFIG['password'],
            account=SNOWFLAKE_CONFIG['account'],
            warehouse=SNOWFLAKE_CONFIG['warehouse'],
            database=SNOWFLAKE_CONFIG['database'],
            schema='dbt_sjbrown'  # Since the schema is dbt_sjbrown for dbt
        )
        cur = con.cursor()
        
        # Check files in the external stage
        cur.execute("LIST @HEALTHCARE_RAW.external_stages.aws_stage")
        files = cur.fetchall()
        
        processed_files = get_processed_files('snowflake-emr', 'processed/')
        
        for file in files:
            file_name = file[0]

            # Clean up file_name
            if "s3://" in file_name:
                file_name = file_name.split('/')[-1]

            if is_processed(file_name, processed_files):
                logging.warning(f"{file_name} has already been processed. Skipping...")
                continue

            # If table_name is valid, proceed with data loading
            load_data_from_stage_to_table(cur, file_name)

            # After loading, move the file to processed folder in S3
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            source_key = f"raw_files/{file_name}"
            destination_key = f"processed/{file_name.replace('.csv', f'_{timestamp}.csv')}"
            move_file_in_s3('snowflake-emr', source_key, 'snowflake-emr', destination_key)

        # Close the cursor and connection at the end
        cur.close()
        con.close()
    except Exception as e:
        logging.error(f"Error during data processing: {e}")



# Test the load_data function
# load_data()  # Commented out the direct test call. You can uncomment it if needed for direct testing.