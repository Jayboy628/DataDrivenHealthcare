import snowflake.connector
from config import SNOWFLAKE_CONFIG

def test_snowflake_connection():
    try:
        con = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG['user'],
            password=SNOWFLAKE_CONFIG['password'],
            account=SNOWFLAKE_CONFIG['account'],
            warehouse=SNOWFLAKE_CONFIG['warehouse'],
            database=SNOWFLAKE_CONFIG['database'],
            schema=SNOWFLAKE_CONFIG['schema']
        )

        # Create a cursor object.
        cur = con.cursor()

        # Run a sample query.
        cur.execute("SELECT CURRENT_VERSION()")
        row = cur.fetchone()
        print("Snowflake version:", row[0])

        # Close the cursor and connection.
        cur.close()
        con.close()

    except Exception as e:
        print("Error:", e)


test_snowflake_connection()

