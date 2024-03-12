import snowflake.connector
from config.config import SNOWFLAKE_CONFIG

print("Starting troubleshooting script...")

def troubleshoot_list_files_in_stage():
    conn = None
    cur = None
    print("Before connecting to Snowflake...")

    try:
        print("Attempting to connect to Snowflake...")
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG.get('user'),
            password=SNOWFLAKE_CONFIG.get('password'),
            account=SNOWFLAKE_CONFIG.get('account'),
            warehouse=SNOWFLAKE_CONFIG.get('warehouse'),
            database=SNOWFLAKE_CONFIG.get('database'),
            schema=SNOWFLAKE_CONFIG.get('schema'),
            role=SNOWFLAKE_CONFIG.get('role', None),
        )
        print("Connected successfully!")

        cur = conn.cursor()
        print("Cursor established...")

        # Set the database and schema explicitly
        cur.execute("USE DATABASE MANAGE_DB;")
        cur.execute("USE SCHEMA external_stages;")

        print("Executing LIST @aws_stage query...")
        
        # Use the fully qualified stage name for clarity
        cur.execute("LIST @MANAGE_DB.external_stages.aws_stage;")
        files = cur.fetchall()

        if not files:
            print("No files found in the Snowflake stage.")
        else:
            print(f"Found {len(files)} file(s) in the Snowflake stage:")
            for file in files:
                print(file)

    except Exception as e:
        if isinstance(e, snowflake.connector.errors.ProgrammingError):
            print(f"Snowflake ProgrammingError: {e}")
        else:
            print(f"General Error: {e}")

    finally:
        if cur and not cur.is_closed():
            print("Closing cursor...")
            cur.close()
        if conn and not conn.is_closed():
            print("Closing connection...")
            conn.close()

# Call the function to execute it when the script runs
troubleshoot_list_files_in_stage()
