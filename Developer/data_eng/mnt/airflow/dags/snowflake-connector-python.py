import snowflake.connector

def test_snowflake_connection():
    # Create a connection object
    con = snowflake.connector.connect(
        user='rayboy',
        password='@Password78!',
        account='shxnusq-bbb65355',
        warehouse='HEALTHCARE_WH',
        database='HEALTHCARE_RAW',
        schema='public'
    )
    
    # Run a simple query
    cur = con.cursor()
    cur.execute("SELECT CURRENT_VERSION();")
    result = cur.fetchone()
    
    # Print the result
    print("Snowflake version:", result[0])
    
    # Close the connection
    con.close()

test_snowflake_connection()

