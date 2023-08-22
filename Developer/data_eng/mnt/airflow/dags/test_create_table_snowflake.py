import pandas as pd
import snowflake.connector

# Sample dataframe
data = {
    'Name': ['John', 'Anna', 'Ella'],
    'Age': [28, 22, 32],
    'Date': ['2021-05-01', '2021-05-02', '2021-05-03']
}
df = pd.DataFrame(data)
df['Date'] = pd.to_datetime(df['Date'])

# Define the function to convert pandas dtype to Snowflake dtype
def pandas_dtype_to_snowflake_dtype(dtype):
    if "int" in str(dtype):
        return "INTEGER"
    elif "float" in str(dtype):
        return "FLOAT"
    elif "datetime" in str(dtype):
        return "TIMESTAMP"
    else:
        return "STRING"

# Function to create a table from the dataframe
def create_table_from_df(cursor, df, table_name):
    columns = ", ".join([f"{col} {pandas_dtype_to_snowflake_dtype(dtype)}" for col, dtype in zip(df.columns, df.dtypes)])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
    print(f"Executing SQL: {create_table_sql}")  # Log the SQL being executed
    cursor.execute(create_table_sql)

# Snowflake setup (please replace the placeholders with your actual Snowflake details)
con = snowflake.connector.connect(
    user='rayboy',
    password='@Password78!',
    account='shxnusq-bbb65355',
    warehouse='HEALTHCARE_WH',
    database='HEALTHCARE_RAW',
    schema='PUBLIC'
)
cur = con.cursor()

try:
    # Test the function
    create_table_from_df(cur, df, 'test_table')
    con.commit()  # Commit the transaction to make sure changes persist
    
    # Check if table was created
    cur.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TEST_TABLE'")
    result = cur.fetchall()
    if result:
        print("Table 'test_table' was successfully created.")
    else:
        print("Table 'test_table' was NOT created.")

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the cursor and connection
    cur.close()
    con.close()

