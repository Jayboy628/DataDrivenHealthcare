
import snowflake.connector
import pandas as pd
import numpy as np

# Sample DataFrame
technologies = {
    'Courses': ["Spark", "PySpark", "Hadoop", "Python"],
    'Fee': [22000, 25000, np.nan, 24000],
    'Duration': ['30day', None, '55days', np.nan],
    'Discount': [1000, 2300, 1000, np.nan]
}
df = pd.DataFrame(technologies)

# Convert DataFrame to CSV
csv_file = "temp_file.csv"
df.to_csv(csv_file, index=False)

conn = snowflake.connector.connect(
    account = 'shxnusq-bbb65355',
        user = 'rayboy',
        password = '@Password78!',
        database = 'healthcare_raw',
        schema = 'public',
        warehouse = 'healthcare_wh',
        role='accountadmin'
)

try:
    cur = conn.cursor()

    # Create the table if it doesn't exist
    table_creation_sql = """
    CREATE TABLE IF NOT EXISTS technologies (
        Courses STRING,
        Fee FLOAT,
        Duration STRING,
        Discount FLOAT
    );
    """
    cur.execute(table_creation_sql)

    # Create a temporary stage
    cur.execute("CREATE OR REPLACE TEMPORARY STAGE my_stage")

    # Upload CSV to stage
    cur.execute(f"PUT file://{csv_file} @my_stage")

    # Copy data from stage to table
    cur.execute("COPY INTO technologies FROM @my_stage FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1)")

    print("Data successfully loaded into Snowflake.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    cur.close()
    conn.close()
