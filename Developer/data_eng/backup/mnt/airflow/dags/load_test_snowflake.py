import snowflake.connector

def test_snowflake_connection():
    # Create a connection object
    con = snowflake.connector.connect(
        user='rayboy',
        password='@Password78!',
        account='shxnusq-bbb65355',
        warehouse='HEALTHCARE_WH',
        database='HEALTHCARE_RAW',
        role='ACCOUNTADMIN',
        schema='public'
    )

    # Use the COPY INTO command to load data
    target_table = "TEST_LOAD"
    stage_name = "@aws_stage"
    file_name = "TEST_LOAD.csv"
    
    sql = f"""
    COPY INTO {target_table}
    FROM {stage_name}/{file_name}
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
    ON_ERROR = 'CONTINUE'; 
    """
    # Execute the command
    cur = con.cursor()
    cur.execute(sql)
    
    # Print a success message
    print(f"Data loaded from {file_name} to Snowflake.")

    # Close the connection
    con.close()

# Call the function to start the test
test_snowflake_connection()

