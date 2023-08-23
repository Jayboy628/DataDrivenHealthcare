import snowflake.connector

def test_snowflake_connection():
    # Create a connection object
    con = snowflake.connector.connect(
        user='manually_enter_for_test',
        password='manually_enter_for_test',
        account='manually_enter_for_test',
        warehouse='manually_enter_for_test',
        database='manually_enter_for_test',
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

