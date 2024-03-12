

import logging
import re
import snowflake.connector
from config.config import SNOWFLAKE_CONFIG, SCHEMA_FILE_MAPPING
from scripts.s3_utils import fetch_files_from_s3

logging.basicConfig(level=logging.INFO)

def load_data_into_snowflake(s3_file_path, schema_name, table_name):
    conn = None
    try:
        logging.info(f"Starting load_data_into_snowflake for {schema_name}.{table_name} with file {s3_file_path}")
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG['user'],
            password=SNOWFLAKE_CONFIG['password'],
            account=SNOWFLAKE_CONFIG['account'],
            warehouse=SNOWFLAKE_CONFIG['warehouse'],
            database=SNOWFLAKE_CONFIG['database_raw'],
            role=SNOWFLAKE_CONFIG['role']
        )
        logging.info("Connected to Snowflake")
        
        # Check if the schema and table are defined in the mapping
        found = False
        for current_schema, tables in SCHEMA_FILE_MAPPING.items():
            if schema_name == current_schema and table_name in tables:
                found = True
                break
        
        if not found:
            raise ValueError(f"{schema_name}.{table_name} not found in SCHEMA_FILE_MAPPING")
        
        # Execute the COPY command
        copy_sql = f"""
            COPY INTO {SNOWFLAKE_CONFIG['database_raw']}.{schema_name}.{table_name}
            FROM @{SNOWFLAKE_CONFIG['database']}.external_stages.aws_stage/{s3_file_path}
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
            ON_ERROR = 'CONTINUE';
        """
        logging.info(f"Executing COPY command: {copy_sql}")
        cur = conn.cursor()
        cur.execute(copy_sql)
        logging.info("COPY command executed successfully")
        
    except Exception as e:
        logging.exception("Failed to load data into Snowflake", exc_info=True)
        raise RuntimeError(f"Failed to load data to Snowflake: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Snowflake connection closed")


def airflow_task_load_data(**kwargs):
    logging.info(f"Full kwargs: {kwargs}")
    # Directly use kwargs to get task_instance or other context variables
    task_instance = kwargs.get('task_instance')

    if task_instance is None:
        logging.error("task_instance is None")
        raise ValueError("task_instance is None. Expected a valid task_instance object, but received None.")

    try:
        logging.info(f"SCHEMA_FILE_MAPPING: {SCHEMA_FILE_MAPPING}")

        s3_file_paths = fetch_files_from_s3()

        if not s3_file_paths:
            logging.warning("No files found in S3. Exiting task.")
            return

        logging.info(f"All S3 file paths: {s3_file_paths}")

        s3_file_paths = [path for path in s3_file_paths if not path.endswith('/')]

        load_count = {}

        for s3_file_path in s3_file_paths:
            logging.info(f"Processing file: {s3_file_path}")
            try:
                match = re.match(r".*\/(\w+)_\d+-\d+.csv", s3_file_path)
                if match:
                    table_name = match.group(1)
                    schema_name = next((key for key, value in SCHEMA_FILE_MAPPING.items() if table_name in value), None)
                    if schema_name:
                        logging.info(f"Loading data for table: {table_name}")
                        load_data_into_snowflake(s3_file_path, schema_name, table_name)

                        schema_table = f"{schema_name}.{table_name}"
                        load_count[schema_table] = load_count.get(schema_table, 0) + 1
                    else:
                        logging.error(f"Table {table_name} not found in SCHEMA_FILE_MAPPING: {s3_file_path}")
                else:
                    logging.error(f"Unable to extract table name from file path: {s3_file_path}")
            except Exception as e:
                logging.error(f"Error processing file {s3_file_path}: {str(e)}", exc_info=True)
        if load_count:
        # Push to XCom directly without 'context' dict
            task_instance.xcom_push(key='load_count', value=load_count)
        else:
            logging.warning("No files were loaded.")

    except Exception as e:
        logging.error(f"Error fetching S3 file paths: {str(e)}", exc_info=True)
