import snowflake.connector
from config.config import SNOWFLAKE_CONFIG, SLACK_CONFIG, get_ssm_parameter

import sys
sys.path.append('/opt/airflow/dags')


# Test SSM connection
def test_ssm_connection():
    test_param = "/snowflake/account"
    value = get_ssm_parameter(test_param)
    if not value:
        raise RuntimeError(f"Failed to fetch value for {test_param}. Check SSM connection.")
    print(f"Successfully fetched value for {test_param}. SSM connection is OK.")

# Test Snowflake connection
def test_snowflake_connection():
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_CONFIG['user'],
            password=SNOWFLAKE_CONFIG['password'],
            account=SNOWFLAKE_CONFIG['account'],
            warehouse=SNOWFLAKE_CONFIG['warehouse'],
            database=SNOWFLAKE_CONFIG['database'],
            schema=SNOWFLAKE_CONFIG['schema']
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        one_row = cur.fetchone()
        print(f"Snowflake version: {one_row[0]}")
    except Exception as e:
        raise RuntimeError(f"Failed to connect to Snowflake: {e}")
    finally:
        if conn:
            conn.close()

# Test Slack token retrieval from SSM
def test_slack_ssm_parameter():
    slack_token = SLACK_CONFIG['token']
    if not slack_token:
        raise RuntimeError("Failed to fetch Slack token from SSM.")
    print("Successfully fetched Slack token from SSM.")

# Execute tests if this script is run as main
if __name__ == "__main__":
    test_ssm_connection()
    test_snowflake_connection()
    test_slack_ssm_parameter()
