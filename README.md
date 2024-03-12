<!-- ABOUT THE PROJECT -->

$${\color{red}Still \space working \space on \space project}$$

## <center>$${\color{blue}Overcoming \space EMR \space Challenges \space with \space Cloud-Based \space Solutions:}$$</center>
<br>
<img src="images/main.png" alt="header" style="width: 900px; height: 400px;"><br>

#### <font color="blue"><em><center>Harnessing Cloud Technology for an Efficient Data Warehouse Solution</em></center></font>
I worked for a health company that encountered a major issue with their EMR system because it did not align with their business process. In turn, this caused the system to be buggy, as too many custom builds were implemented. The company decided to move away from their current system and instead implemented eClinicalWorks. The EMR company owned the database, so my company had to arrange an amendment to the contract that enables them to extend their usage agreement. The EMR company also agreed to FTP the live data files before work begins at 2:00 am and after work ends at 7:00 pm.
<br><br>
My job was to design and implement a data warehouse from these files. The requirements included creating various production reports and KPI’s that matched with the EMR system. The business owners would compare eClinicalWorks integrated reports with my reports and if aligned, they would be flagged to be used for production. In the company’s view, this was critical for data migration because it guaranteed that all operational reports would be correct and more importantly, would prove that eClinicalWorks was configured based on the company’s business requirements.
<br><br>
My intention with this project is to replicate some of the more important aspects of the above scenario. Please note that the healthcare dataset is fake and is being used only for demonstration purposes.

---------------------------------------------------------------------------------------------------------------------


## Cloud Technology Project

A comprehensive guide on setting up a data pipeline leveraging key cloud technologies: 
[Apache Airflow](https://airflow.apache.org/), [Slack](https://slack.com/), [AWS S3](https://aws.amazon.com/),[SODA](https://www.soda.com/),[Snowflake](https://www.snowflake.com/en/), [COSMOS](https://www.astronomer.io/cosmos/), [DBT](https://www.getdbt.com/) and [Tableau](https://www.tableau.com/).

---
### AGENDA
- [Data Modeling](https://towardsdatascience.com/data-modelling-for-data-engineers-93d058efa302)
  - Star Schema
- AWS Environment Setup:`Optional AWS CLI`
  - Create user
  - Create admin group
  - Create S3 bucket
  - Systems Manager Parameter: Optinal
- Snowflake Setup
  - Create Databases
  - Create Roles
  - Privilages
- DBT Setup:
  - Create Profile
- [Data Quality](https://www.montecarlodata.com/blog-data-quality-checks-in-etl/): example
  - Null values tests
  - Volume tests
  - Numeric distribution tests
  - Uniqueness tests
  - Referential integrity test
  - Freshness checks
- Data Lake:example
  - Ingest and Notification
    - Files and Database
      - Raw
- Data Warehouse
  - Stage
  - Transform
- Report
  
### Configuration Approach

The Configuration Approach ensures that all data pipeline components are appropriately set up.

#### Components:

- **Airflow**: Manages ETL workflows.
- **AWS S3**: Acts as data storage.
- **AWS Systems Manager Parameter Store**: Secures configurations.`optional`
- **SODA**:Data Quality.
- **Snowflake**: Our cloud data warehouse.
- **COSMOS**: Integrate with DBT allow airflow to run
- **DBT**: Handles data transformations.
- **Slack**: Delivers process notifications.
- **Tableau**: Deliver reports and Dashboard


---

### 1. Modern Data Moderling

<details>
<summary>Click to Expand</summary>

#### 1. Introduction:

- **Overview**: The data model approach we will use is the Ralph Kimball.


#### 7. Execution:

- **Change Directory where airflow is located**: `cd data_eng/`

   - **Start.sh**:Build the base images from which are based the Dockerfiles (hen Startup all the containers at once )
     ```shell
      docker-compose up -d --build
     ```

   - **Stop.sh**: Stop all the containers at once
     ```shell
      docker-compose down
     ```

   - **Restart.sh**:
     ```shell
     ./stop.sh
     ./start.sh
     ```

   - **reset.sh**:
     ```shell
      docker-compose down
      docker system prune -f
      docker volume prune -f
      docker network prune -f
      rm -rf ./mnt/postgres/*
      docker rmi -f $(docker images -a -q)
     ```
</details>


### 2. AWS Environment Setup

<details>
<summary>Click to Expand</summary>

#### 1. Basic Environment Configuration:

   - **S3 Bucket**: 
     ```shell
     aws s3api create-bucket --bucket YOUR_BUCKET_NAME --region YOUR_REGION
     ```

   - **IAM User 'testjay'**:
     ```shell
     aws iam create-user --user-name testjay
     ```

   - **S3 Bucket Policy**:
     ```shell
     aws iam list-policies
     aws iam attach-user-policy --user-name jay --policy-arn YOUR_BUCKET_POLICY_ARN
     ```

   - **IAM Role 'developer'**:
     ```shell
     aws iam create-role --role-name developer --assume-role-policy-document '{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Principal": {"Service": "ec2.amazonaws.com"},"Action": "sts:AssumeRole"}]}'
     ```

   - **SSM Policy for Role**:
     ```shell
     aws iam list-policies
     aws iam attach-role-policy --role-name developer --policy-arn YOUR_SSM_POLICY_ARN
     ```

   - **Associate Role to User**:
     ```shell
     aws iam put-user-policy --user-name jay --policy-name AssumeDeveloperRole --policy-document '{"Version": "2012-10-17","Statement": [{"Effect": "Allow","Action": "sts:AssumeRole","Resource": "arn:aws:iam::YOUR-AWS-ACCOUNT-ID:role/developer"}]}'
     ```

#### 2. EMR Full Access for S3:

   - **Bucket Policy**:
     ```json
     {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "elasticmapreduce.amazonaws.com"
                },
                "Action": "s3:*",
                "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME/*"
            }
        ]
     }
     ```

   - **Apply Policy**:
     ```shell
     aws s3api put-bucket-policy --bucket YOUR_BUCKET_NAME --policy file://path/to/your/emr-policy.json
     ```

#### 3. AWS Systems Manager Parameter Store:

   - **Parameter Setup**:
     ```shell
     aws ssm put-parameter --name "SnowflakeUsername" --type "String" --value "YourUsername"
     aws ssm put-parameter --name "SnowflakePassword" --type "SecureString" --value "YourPassword"
     aws ssm put-parameter --name "SnowflakeAccount" --type "String" --value "YourAccount"
     aws ssm put-parameter --name "SnowflakeRole" --type "String" --value "YourRole"
     ```

</details>

### 3. Snowflake Setup

<details>
<summary>Click to Expand</summary>

#### 1. Starting with Snowflake:

   - Snowflake offers a cloud-native data platform.
      - [Register on Snowflake](https://www.snowflake.com/)
      - Choose 'Start for Free' or 'Get Started'.
      - Complete the registration.
      - Use credentials to access Snowflake's UI.

#### 2. Structure Configuration:

   - **Data Warehouse**:
     ```sql
     CREATE WAREHOUSE IF NOT EXISTS my_warehouse 
        WITH WAREHOUSE_SIZE = 'XSMALL' 
        AUTO_SUSPEND = 60 
        AUTO_RESUME = TRUE 
        INITIALLY_SUSPENDED = TRUE;
     ```

   - **Database**:
     ```sql
     CREATE DATABASE IF NOT EXISTS my_database;
     ```

   - **Roles and Users**:
     ```sql
     -- Role Creation
     CREATE ROLE IF NOT EXISTS my_role;
     
     -- User Creation
     CREATE USER IF NOT EXISTS jay 
        PASSWORD = '<YourSecurePassword>' 
        DEFAULT_ROLE = my_role
        MUST_CHANGE_PASSWORD = FALSE;
     ```

#### 3. Organize Data:

   - **Schemas**:
     ```sql
     USE DATABASE my_database;
     CREATE SCHEMA IF NOT EXISTS chart;
     CREATE SCHEMA IF NOT EXISTS register;
     CREATE SCHEMA IF NOT EXISTS billing;
     ```

   - **Tables**:
     ```sql
     -- Chart Schema
     CREATE TABLE IF NOT EXISTS chart.code (
        id INT AUTOINCREMENT PRIMARY KEY
     );

     -- Register Schema
     CREATE TABLE IF NOT EXISTS register.users (
        id INT AUTOINCREMENT PRIMARY KEY,
        name STRING,
        email STRING UNIQUE
     );
     ```

#### 4. Permissions:

   - **Assign Roles and Grant Privileges**:
     ```sql
     GRANT ROLE my_role TO USER jay;
     GRANT USAGE ON DATABASE my_database TO ROLE my_role;
     GRANT USAGE ON WAREHOUSE my_warehouse TO ROLE my_role;
     GRANT USAGE ON SCHEMA chart TO ROLE my_role;
     GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA chart TO ROLE my_role;
     ```

**Note**: Replace placeholders (like `<YourSecurePassword>`) with actual values.

</details>

### 4. DBT (Data Build Tool) Setup


<details>
<summary>Click to Expand</summary>

**Note**: DBT (Data Build Tool) provides a means to transform data inside your data warehouse. With it, analytics and data teams can produce reliable and structured data sets for analytics.

   - **Installation**: To get started with DBT, you first need to install it 

     ```shell
      pip install dbt
     ```

   - **Initialize a New DBT Project**: Navigate to your directory of choice and initiate a new project

     ```shell
     dbt init your_project_name
     ```

   - ** Configuration**: Modify the ~/.dbt/profiles.yml to set up your Snowflake connection. This file will contain details such as account name, user, password, role, database, and warehouse.
     ```shell
        your_project_name:
      target: dev
      outputs:
        dev:
          type: snowflake
          account: your_account
          user: your_username
          password: your_password
          role: your_role
          database: your_database
          warehouse: your_warehouse
          schema: your_schema
          threads: [desired_number_of_threads]
     ```
     - **Running and Testing:

     ```shell
     dbt debug
     ```
</details>

### 5. Data Quality (SodaCL ) 


<details>
<summary>Click to Expand</summary>

**Note**: Along with `Data Quality Check` we should implement data [observability](https://www.montecarlodata.com/blog-what-is-data-observability/). `Barr Moses CEO and CO-founder of Monte Carlo` coind "Data observability" She explaind that Data observability provides full visibility into the health of your data AND data systems so you are the first to know when the data is wrong, what broke, and how to fix it.
- **The five pillars of data observability:** 
  - Freshness
  - Quality 
  - Volume 
  - Schema
  - Lineage
    
### Data Quality Checks
 - **1) Null Values Tests**: These checks ensure that essential columns do not contain null values. For the `dim_provider` table, it's crucial that primary key, NPI, first and last names, specialty, and email don't have nulls as they are essential for identifying and contacting the provider.
   ```shell
    checks for dim_provider:
      - missing_count(PROVIDER_PK) = 0
      - missing_count(PROVIDER_NPI) = 0
      - missing_count(FIRST_NAME) = 0
      - missing_count(LAST_NAME) = 0
      - missing_count(PROVIDER_SPECIALTY) = 0
      - missing_count(EMAIL) = 0
   ```
 - **2) Volume Tests**: Volume tests ensure the table contains a reasonable number of records, which can indicate whether the data loading process worked correctly.
     ```shell
      checks for dim_provider:
        - row_count between 100 and 10000
     ```
 - **3) Numeric Distribution Tests**: These tests can validate that numeric columns like `AGE` have values within expected ranges and distributions.
     ```shell
      checks for dim_provider:
        - invalid_percent(AGE) < 5%:
            valid min: 25
            valid max: 100
     ```
 - **4) Uniqueness Tests**: Uniqueness tests verify that columns that should be unique, such as `PROVIDER_NPI` and `EMAIL`, do not have duplicate values.
     ```shell
      checks for dim_provider:
        - duplicate_count(PROVIDER_NPI) = 0
        - duplicate_count(EMAIL) = 0
     ```     
- **5) Referential Integrity Test**: These tests ensure that values in a column match values in a column in another table, for example, ensuring that the `PROVIDER_SPECIALTY` exists in a `dim_specialty` table.
   ```shell
    checks for dim_provider:
      - duplicate_count(PROVIDER_NPI) = 0
      - duplicate_count(EMAIL) = 0
   ```

- **6) Freshness Checks**: Freshness checks are useful for tables that are expected to be updated regularly. If your table includes a timestamp column (not shown in your schema), you could implement a check like:
    ```shell
      checks for dim_provider:
        - freshness (LAST_UPDATE_TIMESTAMP) < 2d
    ``` 

</details>


## Ingestion Approach for Data Lake

Our Ingestion Approach is designed to ensure that all data pipeline components are appropriately set up and functioning as intended.

---
### Airflow: Orcahstrate the following:
  - **Sources**: ingest data into raw_files folder (S3 buckets) and `alert`.
    - **Folder Management and Notification**:
      - errors: error files store in the error_folder `Slack alert`.
      - processed: processed data ingest into snowflake `Slack alert`


### 1. Project Environment

<details>
  <summary>Click to Expand: AWS S3 environment</summary>
  
  - Command to list S3 folders: `aws s3 ls s3://snowflake-emr`
     
      ```shell
        PRE error_files/
        PRE processed/
        PRE raw_files/
    ```
#### a. Naming Conventions:
  
  - Timestamps are used for file naming:
  
      ```python
        timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S')
      ```
</details>
<details>
  <summary>Click to Expand: Data Quality checks environment (SODA)</summary>
  
  - **Command to list** : `ls include/soda/` and `ls include/soda/checks/`
     
      ```shell
        stro@cfabfee5ced1:/usr/local/airflow$ ls include/soda/
      __pycache__  check_function.py  check_transform.py  checks  config.py  configuration_bill.yml  configuration_chart.yml  configuration_register.yml  configuration_transform.yml
        astro@cfabfee5ced1:/usr/local/airflow$ ls include/soda/checks/
        bill_tables  chart_tables  register_tables  transform
        astro@cfabfee5ced1:/usr/local/airflow$ ls include/soda/checks/register_tables/
        raw_address.yml  raw_date.yml  raw_location.yml  raw_user.yml
        astro@cfabfee5ced1:/usr/local/airflow$ 
    ```
  - **configuration_register.yml_** : `ls include/soda/` 
     
        ```shell
	      data_source healthcare_db:
	       type: snowflake
	       username: ${SNOWFLAKE_USER}
	       password: ${SNOWFLAKE_PASSWORD}
	       account: ${SNOWFLAKE_ACCOUNT}
	       database: RAW 
	       warehouse: HEALTHCARE_WH
	       connection_timeout: 240
	       role: DEVELOPER
	       client_session_keep_alive: true
	       authenticator: snowflake
	       session_params:
	         QUERY_TAG: soda-queries
	         QUOTED_IDENTIFIERS_IGNORE_CASE: false
	       schema: REGISTER
	      data_source healthcare_dev:
	        type: snowflake
	        username: ${SNOWFLAKE_USER}
	        password: ${SNOWFLAKE_PASSWORD}
	        account: ${SNOWFLAKE_ACCOUNT}
	        database: DEV 
	        warehouse: HEALTHCARE_WH
	        connection_timeout: 240
	        role: DEVELOPER
	        client_session_keep_alive: true
	        authenticator: snowflake
	        session_params:
	          QUERY_TAG: soda-queries
	          QUOTED_IDENTIFIERS_IGNORE_CASE: false
	        schema: DBT_SBROWN
	        soda_cloud:
	        host: cloud.us.soda.io
	        api_key_id: soda id
	        api_key_secret: soda secret
    ```
 - **raw_usser.yml** : `cat include/soda/checks/register_tables/`
    
     ```shell
	   checks for raw_user:
	     - schema:
	         fail:
	           when required column missing: [UIDPK, UID, UFNAME, ULNAME, EMAIL, GENDER, AGE, USERTYPE, UPDATE_AT]
	           when wrong column type:
	             UIDPK: NUMBER
	             UID: NUMBER
	             UFNAME: VARCHAR
	             ULNAME: VARCHAR
	             EMAIL: VARCHAR
	             GENDER: VARCHAR
	             AGE: NUMBER
	             USERTYPE: VARCHAR
	             UPDATE_AT: TIMESTAMP_NTZ
				 ```
</details>
<details>
  <summary>Click to Expand: Slack Notification </summary>
  
  #### a. Slack Notifications:
  
  - Slack webhook integration for notifications on success or failure: **lease ensure you've taken care of the security considerations (like not hardcoding AWS access keys or Slack Webhook URLs) when using these scripts in a real-world scenario. Use environment variables or secrets management tools instead**
  
      ```python
        SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/XXXXXXXXX/XXXXXXXXX/XXXXXXXXXXXXXX'  # Replace with your webhook URL
    
        def send_slack_message(message):
            #... [rest of the code]
      ```
</details>

### 2. Aiflow(Astro) configuration

<details>
<summary>Click to Expand</summary>

#### a. airflow_setting:

- **Overview**:  Automate your Airflow configuration to avoid repetitive setup tasks each time Airflow is initiated. This approach enables configuring Airflow Connections, Pools, and Variables all in one place, specifically tailored for local development environments.
- **Variables to consider**
  - AWS login credentials
  - Snowflake login credentials
  - Slack connection details
  - S3 bucket specifics (name, key, prefix, processed, error paths)
  - Snowflake configurations (tables, schema, databases, stage)
  - Slack notifications (channel, token)
  - Local file path for datasets

- **Configuration in `airflow_setting.yaml`**: This YAML snippet defines essential Airflow configurations for seamless integration with AWS, Snowflake, and Slack, alongside managing S3 buckets and local datasets. Note: Ensure sensitive information like login credentials is securely managed and not hard-coded.

    ```python
      airflow:
        connections:
          - conn_id: aws_default
            conn_type: aws
            login: <aws_access_key_id>
            password: <aws_secret_access_key>
            extra:
              region_name: us-east-1
      
          - conn_id: snowflake_default
            conn_type: snowflake
            login: <your_snowflake_username>
            password: <your_snowflake_password>
            schema: DATA_RAW
            extra:
              account: <your_snowflake_account>
              warehouse: <your_snowflake_warehouse>
              database: <your_snowflake_database>
              role: <your_snowflake_role>
              region: us-east-1
      
          - conn_id: slack_default
            conn_type: slack
            password: <your_slack_bot_token>
      
        variables:
          # Snowflake and S3 configurations
          - variable_name: S3_BUCKET
            variable_value: "your_s3_bucket_name"
      
          # Additional configurations...
      
          # Slack notifications
          - variable_name: SLACK_CHANNEL
            variable_value: "#your_slack_channel"
      
          # Local dataset configurations
          - variable_name: LOCAL_DIRECTORY
            variable_value: "/path/to/your/local/dataset/"
    ```


#### b. Dockerfile Configuration:

- **Overview**: Customize the Dockerfile to include necessary installations for your Airflow environment.
- **Dockerfile Content**: This Dockerfile extends the Astronomer Astro Runtime image, incorporating additional packages and tools required for your workflows, such as AWS CLI, specific Airflow providers, and the Slack SDK.

    ```python
        FROM quay.io/astronomer/astro-runtime:10.4.0

        USER root
        
        # Install AWS CLI
        RUN apt-get update && \
            apt-get install -y awscli && \
            rm -rf /var/lib/apt/lists/*
        
        USER astro
        
        # Install Airflow providers and the Slack SDK
        RUN pip install --no-cache-dir apache-airflow-providers-slack apache-airflow-providers-amazon==8.11.0 slack_sdk
        
        # Configure environment variables for sensitive information
        ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
        
        # Additional configuration...
    ```

#### c. Requirements :

- **Overview**: Define additional dependencies and packages required for your Airflow setup.
    ```python
      # Astro Runtime includes the following pre-installed providers packages: https://docs.astronomer.io/astro/runtime-image-architecture#provider-packages
      astro-sdk-python[amazon, snowflake] >= 1.1.0
      astronomer-cosmos[dbt.snowflake]
      apache-airflow-providers-snowflake==4.4.0
      soda-core-snowflake==3.2.1
      protobuf==3.20.0
    ```
</details>

### 3. Aiflow(Astro) dag

<details>
 <summary>Click to Expand: Load data into data lake </summary>
	
- **Overview**: The DAG defined in your script, `load_file_s3_etl`, is designed to automate the process of uploading CSV files from a local directory to an Amazon S3 bucket, notify a Slack channel about the upload status, and subsequently trigger another DAG for processing the uploaded data. It's structured to run daily without catching up on past executions. Here's an overview of its components and workflow:
- **Configuration**
	- Utilizes Airflow's Variable feature to dynamically set the S3 connection ID, bucket name, key, local directory, and Slack channel.
	- Employs Amazon AWS S3 and Slack providers for interactions with S3 and Slack, respectively.
    ```python
      	from airflow.decorators import dag, task
	from datetime import datetime
	from airflow.providers.amazon.aws.hooks.s3 import S3Hook
	from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
	from airflow.providers.slack.operators.slack import SlackAPIPostOperator
	from airflow.operators.dagrun_operator import TriggerDagRunOperator  # Added import
	from airflow.models import Variable
	import glob
	import os
	import logging

	# Configuration variables
	S3_CONN_ID = 'aws_default'
	S3_BUCKET = Variable.get('S3_BUCKET')
	S3_KEY = Variable.get('S3_KEY').rstrip('*.csv')
	LOCAL_DIRECTORY = Variable.get('LOCAL_DIRECTORY')
	SLACK_CHANNEL = Variable.get('SLACK_CHANNEL')  # Ensure this is set in Airflow Variables
	
	@dag(start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False, tags=['s3', 'slack'])
	def load_file_s3_etl():
	    @task
	    def upload_csv_files_to_s3(bucket: str, key: str, local_directory: str, aws_conn_id: str):
	        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
	        uploaded_files_info = []
	        for filepath in glob.glob(os.path.join(local_directory, '*.csv')):
	            if not os.path.exists(filepath):
	                logging.error(f"File not found: {filepath}")
	                continue
	            filename = os.path.basename(filepath)
	            dest_key = os.path.join(key, filename)
	            s3_hook.load_file(filename=filepath, key=dest_key, bucket_name=bucket, replace=True)
	            file_size = os.path.getsize(filepath)
	            uploaded_files_info.append({'filename': filename, 'size': file_size})
	            os.remove(filepath)
	        return uploaded_files_info
	
	    @task
	    def notify_slack(uploaded_files_info):
	        if uploaded_files_info:
	            message = "Files successfully uploaded to S3:\n"
	            message += "\n".join([f"{file_info['filename']} (Size: {file_info['size']} bytes)" for file_info in uploaded_files_info])
	        else:
	            message = "No new files were uploaded to S3."
	        
	        SlackAPIPostOperator(
	                task_id="notify_slack",
	                channel=SLACK_CHANNEL,
	                text=message,
	                slack_conn_id="slack_default"  # Ensure this Slack connection is configured in Airflow
	            ).execute({})
	
	    
	    trigger_ingest_snowflake = TriggerDagRunOperator(
	    task_id="trigger_ingest_snowflake",
	    trigger_dag_id="dynamic_s3_to_snowflake_etl",
	    execution_date='{{ ds }}',  # Set the execution date explicitly
	    conf={"some_key": "some_value"},  # Any additional configuration
	    reset_dag_run=True,  # Optional: based on your need
	    wait_for_completion=True,  # Optional: based on your need
	    poke_interval=60,  # Optional: default is 60 seconds
	    allowed_states=['success'],  # Optional: default is ['success']
	    failed_states=['failed'],  # Optional
	    deferrable=False,  # Optional: based on your need
	)
	
	
	    uploaded_files_info = upload_csv_files_to_s3(S3_BUCKET, S3_KEY, LOCAL_DIRECTORY, S3_CONN_ID)
	    notify_slack_result = notify_slack(uploaded_files_info)
	    
	    notify_slack_result >> trigger_ingest_snowflake
	
	etl_dag = load_file_s3_etl()
    ```
    #### Task
  	1. `upload_csv_files_to_s3`:
   	   - Scans a specified local directory for CSV files.
           - Uploads each found CSV file to a specified S3 bucket and key location, replacing the file if it already exists.
  	   - Records information about the uploaded files, such as their names and sizes, then deletes the local copies of these files.
  	   - Returns a list of dictionaries, each containing information about an uploaded file.
  	2. `notify_slack`:
  	   - Takes the list of uploaded file information as input.
  	   - If files were uploaded, constructs a message listing the names and sizes of the uploaded files.
  	   - If no files were uploaded, prepares a message indicating that no new files were uploaded.
  	   - Sends the prepared message to a specified Slack channel using the `SlackAPIPostOperator`.
  	3. `trigger_ingest_snowflake`:
  	   - Configured to trigger another DAG, presumably for processing the uploaded files in Snowflake, upon successful completion of the `notify_slack task`.
  	   - Allows for additional configuration via the conf parameter, and offers options to reset the DAG run, wait for completion, and specify poke intervals and allowed/failed states for more controlled execution flow.

   #### Workflow
  	- The DAG starts with the `upload_csv_files_to_s3` task to upload CSV files from the local directory to S3.
  	- Next, the `notify_slack` task executes, sending a notification to Slack about the status of the upload.
  	- Finally, if the notification task succeeds, the trigger_ingest_snowflake task is executed to trigger another DAG for further processing, demonstrating a linear sequence of task dependencies.	  

   #### Features
  	- The DAG includes modern Airflow features, such as the use of the `@dag` and `@task` decorators for simplified DAG and task definitions.
  	- It showcases interaction with external systems (S3 and Slack) and the chaining of workflows via DAG triggering, making it a practical example of a data pipeline that incorporates data uploading, notification, and further data processing steps.
  
</details>

## Transform Approach
Our Ingestion Approach is designed to ensure that all data pipeline components are appropriately set up and functioning as intended.

---
- **Dockerfile**:
    **Best Practice Recommendations**:
      - **Error Handling**: Incorporate try-except blocks for robust error handling.
      - **Logging**: Utilize Python's logging library over simple print statements.
      - **Parameterization**: Make the script versatile to handle any DataFrame and path.
      - **Efficient Data Handling**: Stream data in chunks instead of splitting the dataframe in-memory.
<br>
<img src="images/Dag.png" alt="header" style="width: 900px; height: 400px;"><br>

#### Dags process:

- Design DAGs for different workflows – `data ingestion`, `transformation`, and `reporting`.
  - Best practise: Have different Dags for each work flow. `I created one Dag for this project`
  - Set up troubleshooting script:
    - Create `Test Script` before creating your Dags, this ave troubleshooting time: `list A few`
      - **Test python version** `_00_python_version_dag.py`: 
      - **Validate path S3-bucket** `_04_s3_path.py`                   
      - **Connect to Snowflake and validate Snowflake version** `_01_snowflake_connect_connect.py`        
      - **Validate AWS Service Manager Parameter**`_02_snowflake-connector_.py`                           
      - **Validate Create Table in Snowflake**`_03_create_tables_snowflake.py`                   
    - Set up error handling mechanisms in Airflow to handle failures or inconsistencies.
      - ** Error Handling: try and except**
          ```python
          # Copy the file to the new location
          try:
              print(f"Copying from {source_bucket}/{source_key} to {destination_bucket}/{destination_key}")  # Logging
              s3.copy_object(Bucket=destination_bucket, CopySource={'Bucket': source_bucket, 'Key': source_key}, Key=destination_key)
          except Exception as e:
              print(f"Error copying object: {e}")
              return  # Exit the function if copying fails
          ```
  - Implement logging and monitoring to keep track of DAG runs:`Did not implement however used slack for alert`
  - Implement retries and alert mechanisms in case of DAG failures:`Did not implement however used slack for alert`
  - Ensure there's a solid connection setup between Airflow and Snowflake.
    - `docker-compose.yml`:Ensure that your local devlopment matches the airflow(AWS,DBT):`opt/airflow`
    ```python
        airflow:
          build: ./docker/airflow
          restart: always
          container_name: airflow
          volumes:
            - ./mnt/airflow/airflow.cfg:/opt/airflow/airflow.cfg
            - ./mnt/airflow/dags:/opt/airflow/dags
            - ~/.aws:/opt/airflow/.aws
            - /home/yourpath/.dbt:/opt/airflow/.dbt
            - /home/yourpath/projects/repo/DataDrivenHealthcare:/app
    ```
  - **Dag has a list of Task**:`DataDriven_Healthcare:`
      - **start_task**:`Airflow.operators.dummy_operator:Its like placeholder can be use to start task`:This module is deprecated. Please use `airflow.operators.empty`
          ```python
              start_task = DummyOperator(task_id='start_task')
          ```
      - **list_files_in_raw_files**:
          ```python
              s3_list = S3ListOperator(
              task_id='list_files_in_raw_files',
              bucket='snowflake-emr',
              prefix='raw_files/',
              aws_conn_id='aws_default'
              )
          ```
      - **branch_check_raw_files**: When the `BranchPythonOperator` is executed, it will run the provided Python callable. The callable should return the task_id of the next task to run. After the `BranchPythonOperator` task completes, the task with the task_id returned by the Python callable will be executed, while all other downstream tasks are skipped.
          ```python
                branch = BranchPythonOperator(
                task_id='branch_check_raw_files',
                python_callable=branch_based_on_files_existence,
                provide_context=True,
                op_args=[],
                op_kwargs={'bucket_name': 'snowflake-emr', 'prefix': 'raw_files/'})
          ```
      - **load_to_snowflake**: `from airflow.operators.python_operator import PythonOperator`:The PythonOperator is a specific type of operator used to execute a Python callable (a function) within a DAG
          ```python
                start_task = DummyOperator(task_id='start_task')
          ```
      - **run_stage_models**: The `S3ListOperator` is an operator in Apache Airflow that's used to get a list of keys from an S3 bucket.
          ```python
                run_stage_models = BashOperator(
                task_id='run_stage_models',
                bash_command='/app/Developer/dbt-env/bin/dbt run --model tag:"DIMENSION" --project-dir /app/Developer/dbt_health --profile dbt_health --target dev')
          ```
      - **slack_success_alert**: 
          ```python
                slack_success_alert=task_success_slack_alert(dag=dag)
          ```
      - **end_task**
          ```python
                end_task = DummyOperator(task_id='end_task')
          ```
