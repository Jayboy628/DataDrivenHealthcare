def main():
    try:
        from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
        print("S3ToSnowflakeOperator is available!")
    except ImportError:
        print("S3ToSnowflakeOperator is not available.")


if __name__ == "__main__":
    main()

