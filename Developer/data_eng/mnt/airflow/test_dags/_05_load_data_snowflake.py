import snowflake.connector

def test_snowflake_connection():
    # Create a connection object
    con = snowflake.connector.connect(
        user='manually_enter_for_test',
        password='manually_enter_for_test',
        account='manually_enter_for_test',
        warehouse='manually_enter_for_test',
        database='manually_enter_for_test',
        role='manually_enter_for_test',
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

