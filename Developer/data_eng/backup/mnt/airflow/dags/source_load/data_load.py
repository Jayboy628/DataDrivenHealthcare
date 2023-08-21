import logging

import snowflake.connector
from config import SNOWFLAKE_CONFIG
import boto3
from datetime import datetime

from config import SNOWFLAKE_CONFIG, SNOWFLAKE_TABLES

def load_data_from_stage_to_table(cur, file_name):
    try:
        # Extract the table name from the file name
        table_name = file_name.split('.')[0].upper()
        
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
            schema='public'  # Since the schema is public
        )
        cur = con.cursor()
        
        # Check files in the external stage
        cur.execute("LIST @HEALTHCARE_RAW.external_stages.aws_stage")
        files = cur.fetchall()
        
        for file in files:
            file_name = file[0]

            # Clean up file_name
            if "s3://" in file_name:
                file_name = file_name.split('/')[-1]

            # Extract table name from file_name
            table_name = file_name.split('.')[0].upper()

            # Check if table_name exists in Snowflake directly
            # Query all tables from the schema
            table_check_sql = f"SHOW TABLES IN SCHEMA {SNOWFLAKE_CONFIG['database']}.public"
            cur.execute(table_check_sql)
            all_tables = [row[1] for row in cur.fetchall()]  # Assuming the table name is in the second position of the result

            # Check in Python if the desired table exists in the list
            if table_name not in all_tables:
                # If the table doesn't exist, move the file to the error_files folder and continue to the next file
                destination_key = f"error_files/{file_name}"
                move_file_in_s3('snowflake-emr', f"raw_files/{file_name}", 'snowflake-emr', destination_key)
                print(f"Moved {file_name} to error_files as it doesn't match any table.")
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
