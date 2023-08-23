import boto3

def get_table_names_from_s3(bucket_name, prefix):
    s3 = boto3.client('s3')
    
    try:
        # Listing the objects under the provided prefix
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in objects:
            print("No objects found with the provided prefix.")
            return []
        
        # Extract table names from the object keys
        table_names = []
        for obj in objects['Contents']:
            object_key = obj['Key']
            file_name = object_key.split('/')[-1]
            table_name = file_name.split('.')[0]
            table_names.append(table_name)
        
        return table_names
    
    except Exception as e:
        print(f"Error encountered: {e}")
        return []

# Testing the function
bucket_name = 'snowflake-emr'
prefix = 'Chart/'  # this is the folder under which your files are
table_names = get_table_names_from_s3(bucket_name, prefix)
if table_names:
    for table in table_names:
        print(table)  # Should print the table names, extracted from file names
else:
    print("Failed to get the table names.")

