<!-- ABOUT THE PROJECT -->



## <center>$${\color{blue}Overcoming \space EMR \space Challenges \space with \space Cloud-Based \space Solutions:}$$</center>
<br>
<img src="images/main.png" alt="header" style="width: 900px; height: 400px;"><br>

#### <font color="blue"><em><center>Harnessing Cloud Technology for an Efficient Data Warehouse Solution</em></center></font>
I worked for a health company that encountered a major issue with their EMR system because it did not align with their business process. In turn, this caused the system to be buggy, as too many custom builds were implemented. The company decided to move away from their current system and instead implemented eClinicalWorks. The EMR company owned the database, so my company had to arrange an amendment to the contract that enabled them to extend their usage agreement. The EMR company also agreed to FTP the live data files before work begins at 2:00 am and after work ends at 7:00 pm.
<br><br>
My job was to design and implement a data warehouse from these files. The requirements included creating various production reports and KPI’s that matched with the EMR system. The business owners would compare eClinicalWorks integrated reports with my reports and if aligned, they would be flagged to be used for production. In the company’s view, this was critical for data migration because it guaranteed that all operational reports would be correct and more importantly, would prove that eClinicalWorks was configured based on the company’s business requirements.
<br><br>
My intention with this project is to replicate some of the more important aspects of the above scenario. Please note that the healthcare dataset is fake and is being used only for demonstration purposes.

---------------------------------------------------------------------------------------------------------------------


## Cloud Technology Project

A comprehensive guide on setting up a data pipeline leveraging key cloud technologies: 
[Apache Airflow](https://airflow.apache.org/), [Docker](https://www.docker.com/), [Slack](https://slack.com/), [AWS S3](https://aws.amazon.com/),[SODA](https://www.soda.com/),[Snowflake](https://www.snowflake.com/en/), [COSMOS](https://www.astronomer.io/cosmos/), [DBT](https://www.getdbt.com/) and [Tableau](https://www.tableau.com/).

---
### AGendA
1. Setup Environment
	- AWS Environment Setup (Optional: AWS CLI)
	- Snowflake Setup
	- Tool Configuration
2. Design and Planning
	- Data Modeling
	- Data Quality Planning
3. Operational Tasks
	- Data Ingestion and Notification
	- Data Transformation with DBT
	- Data Quality Checks with SODA
	- Reporting and Visualization
	- Workflow Automation and Communication
 
  
--- 

### Data Warehouse Architecture Approach
<br>
<img src="images/ModernDataWarehouse2.png" alt="header" style="width: 1110px; height: 500px;">

#### Components:

- **Airflow**: Orchestrates and manages ETL workflows.
- **AWS S3**: Acts as data storage.
- **AWS Systems Manager Parameter Store**: Secures configurations.`optional`
- **SODA**:Data Quality.
- **Snowflake**: Our cloud data warehouse.
- **COSMOS**: Integrate with DBT allow airflow to run
- **DBT**: Handles data transformations.
- **Slack**: Delivers process notifications.
- **Tableau**: Delivers reports and Dashboard


---

### 1. Setup Environment

<details>
<summary>Click to Expand:AWS Environment Setup (Optional: AWS CLI)</summary>

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

</details>

<details>
<summary>Click to Expand: Snowflake Setup</summary>

#### 1. Starting with Snowflake

   - Snowflake offers a cloud-native data platform.
      - [Register on Snowflake](https://www.snowflake.com/)
      - Choose 'Start for Free' or 'Get Started'.
      - Complete the registration.
      - Use credentials to access Snowflake's UI.

#### 2. Structure Configuration

   - **Data Warehouse**:
     ```sql
     CREATE WAREHOUSE IF NOT EXISTS my_warehouse 
        WITH WAREHOUSE_SIZE = 'XSMALL' 
        AUTO_SUSPend = 60 
        AUTO_REsumE = TRUE 
        INITIALLY_SUSPendED = TRUE;
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

	 - **Show grants for a specific user on databases**: This command confirm what privilages you have
	 SHOW GRANTS TO USER USERS;

	 SHOW GRANTS TO ROLE ;

  - **Create Stage***
 	```sql
	 	// Step 1
	 	USE ROLE YOUR ROLE NAME;
	
	 	// Step 2 create external stage
	 	CREATE SCHEMA IF NOT EXISTS YOUR_MANAGE_DATABASE.SCHEMA;

	 	// Step 3
	 	CREATE OR REPLACE STAGE YOUR_MANAGE_DATABASE.SCHEMA.STAGE_NAME
	     	URL='s3://snowflake-emr/raw_files'
	     	CREDENTIALS=(AWS_KEY_ID='YOUR ID' AWS_SECRET_KEY='YOUR SECRET');

	 	   // view stage
	 	  DESC STAGE YOUR_MANAGE_DATABASE.SCHEMA.stage_name;

	 	 LIST @YOUR_MANAGE_DATABASE.SCHEMA.stage_name;


	 	// create file format
	 	CREATE OR REPLACE FILE FORMAT YOUR_MANAGE_DATABASE.SCHEMA.my_csv_format
	   	TYPE = 'CSV'
	   	FIELD_DELIMITER = ','
	   	SKIP_HEADER = 1
	   	FIELD_OPTIONALLY_ENCLOSED_BY = '"'
	   	NULL_IF = ('NULL', 'null')
	   	EMPTY_FIELD_AS_NULL = TRUE
	   	TRIM_SPACE = TRUE
	   	ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
	   	ESCAPE = 'NONE'
	   	ESCAPE_UNENCLOSED_FIELD = '\\';

	 	SHOW FILE FORMATS IN SCHEMA YOUR_MANAGE_DATABASE.SCHEMA;
	 	SHOW GRANTS ON FILE FORMAT YOUR_MANAGE_DATABASE.SCHEMA.my_csv_format;


	 	SHOW FILE FORMATS;


	 	COPY INTO HEALTHCARE_RAW.DATA_RAW.RAW_LOCATION
	 	FROM '@YOUR_MANAGE_DATABASE.SCHEMA.stage_name/raw_Location.csv'
	 	FILE_FORMAT = (FORMAT_NAME = 'MANAGE_DB.EXTERNAL_STAGES.MY_CSV_FORMAT');
	```
	 

#### 3. Organize Data

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

#### 4. Permissions

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

<details>
<summary>Click to Expand: Tool Configuration</summary>

#### 1. Airflow (Astro)

- **Overview** In This project I am Installing Apache Airflow using the Astronomer CLI (Astro CLI) on Docker in a Mac environment. Before proceeding, ensure Docker Desktop is installed on your Mac. If it's not installed, download it from [Docker Hub](https://hub.docker.com/) and follow the installation instructions.

   - **Step 1: Install the Astro CLI**: The Astro CLI is a command-line tool that makes it easier to run Airflow on your machine. To install the Astro CLI, open your terminal and run the following command:
     ```sql
	 curl -sSL https://install.astronomer.io | sudo bash -s -- v0.25.0
     ```	

   - **Step 2: Initialize an Airflow Project**:Once the Astro CLI is installed, create a new directory for your Airflow project and navigate into it:
     ```sql
	 mkdir your-airflow-project && cd your-airflow-project
     ```	
   - Initialize a new Airflow project using the Astro CLI:This command generates a new Airflow project with a sample DAG file and the necessary configuration files.
     ```sql
	 astro dev init  
     ```	
   - **Step 3: Start Airflow**: This command starts Airflow within a Docker container. It might take a few minutes to pull the required Docker images and start the containers.
     ```sql
	 astro dev start
     ```	
   - **Step 4: Access the Airflow Web Interface**: Once Airflow is running, you can access the Airflow web interface by opening a web browser and navigating to
     ```sql
	 http://localhost:8080/

     ```
	 - Airflow Credential: When you initialize Apache Airflow using the Astronomer CLI (Astro CLI) in a local Docker environment, it automatically configures Airflow with default credentials for you to use when accessing the Airflow web UI. As of Airflow 2.x, the default username and password set by the Astronomer CLI are:U
	 	- Username:`admin` Password `admin`
		- Changing default credentials is a good practise, especially in a production environment.Here's how you can change the default credentials: first -> `astro dev stop`
			1. Locate the `docker-compose.yml` File: This file is in the root of your Airflow project directory created by the `astro dev init` command.
			2. Edit the Environment Variables: Open `docker-compose.yml` in a text editor and look for the environment variables under the `airflow-webserver` service. You'll add or modify the `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` environment variables to set a new username and password.
			```sql
			environment:
			  - AIRFLOW__CORE__EXECUTOR=LocalExecutor
			  - AIRFLOW__WEBSERVER__SECRET_KEY=secret_key
			  - AIRFLOW__WEBSERVER__BASE_URL=http://localhost:8080
			  - _AIRFLOW_WWW_USER_USERNAME=your_new_username
			  - _AIRFLOW_WWW_USER_PASSWORD=your_new_password
			```
			3. Then `astro dev start`**Log in Using New Credentials**: Now, you can use the new username and password you set to log into the Airflow web UI.

#### 2. airflow_setting:

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


#### 3. Dockerfile Configuration:

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

#### 4. Requirements :

- **Overview**: Define additional dependencies and packages required for your Airflow setup.
    ```python
      # Astro Runtime includes the following pre-installed providers packages: https://docs.astronomer.io/astro/runtime-image-architecture#provider-packages
      astro-sdk-python[amazon, snowflake] >= 1.1.0
      astronomer-cosmos[dbt.snowflake]
      apache-airflow-providers-snowflake==4.4.0
      soda-core-snowflake==3.2.1
      protobuf==3.20.0
    ```


#### 5. Slack

   - **Slack**: To use the `SlackAPIPostOperator` in Apache Airflow for sending notifications to a Slack channel, you'll need to set up a Slack App, configure a Slack Connection in Airflow, and then use the operator in your DAG. Here's a step-by-step guide
   
   - **Step1: Create a Slack APP**
    1. Go to the Slack API: Navigate to Your Apps on the Slack API site and click "Create New App".
	2. Name Your App & Choose Workspace: Provide a name for your app and select the Slack workspace where you want to install the app.
	3. Permissions: In the "OAuth & Permissions" section, scroll down to "Scopes" and add the chat:write scope under "Bot Token Scopes". This permission allows your app to send messages as itself.
	4. Install App to Workspace: Click "Install App to Workspace" and authorize the permissions.
	5. Copy Bot User OAuth Token: After installing the app, you'll see a "Bot User OAuth Token" in the "OAuth & Permissions" page. Copy this token; you'll use it to configure the Airflow connection.
	
   - **Step 2: Configure a Slack Connection in Airflow**
    1. Airflow UI: Go to the Airflow web interface.
	2. Connections: Navigate to "Admin" > "Connections", and click on the "+" button to add a new connection.
	3. Connection Details:
		- Conn Id: Enter a unique identifier for this connection, such as slack_default.
		- Conn Type: Select "HTTP".
		- Host: Enter https://slack.com/api/chat.postMessage (Slack's method for sending messages).
		- Password: Paste the "Bot User OAuth Token" you copied earlier.
	4. Save: Click the "Save" button to create the connection
		
  

#### 6. DBT profile creation
- **DBT**:Installing dbt (data build tool) on a Mac and configuring it to work with Snowflake, especially within the context of an Astronomer project (`astro`), involves a few steps. Below is a comprehensive guide that includes installing dbt on your Mac, configuring it for Snowflake, and ensuring it runs within a Docker container managed by Astronomer's CLI tool when you execute astro dev start.


	- **Note**: DBT (Data Build Tool) provides a means to transform data inside your data warehouse. With it, analytics and data teams can produce reliable and structured data sets for analytics.

	   - **Step 1: Install dbt on Mac**: To get started with DBT, you first need to install it
	   
	   	 1. Install Homebrew (if not already installed): Open a terminal and run

	     ```shell
		  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
	     ```
		 2. `Install Python` (if not already installed or if you need a different version):It's recommended to use `pyenv` for managing multiple Python versions:
		 
		 ```shell
		 brew install pyenv
		 pyenv install 3.8.5
		 pyenv global 3.8.5
		 
		 ```
		 3. Install dbt: You can install dbt for Snowflake using pip. It’s a good practice to use a virtual environment
		 
		 ```shell
		 python -m venv dbt_env
		 source dbt_env/bin/activate
		 pip install dbt-snowflake
		 
		 ```
	  - **Step 2: Configure dbt for Snowflake**:
	   
	   	 1. Initialize a dbt Project: Navigate to your project directory and run
		 
		 ```shell
		 dbt init my_dbt_project
		 ```
		 2. Configure profiles.yml: dbt uses a file named profiles.yml for connection configurations. This file is typically located at ~/.dbt/. Edit this file to include your Snowflake credentials:
		 
		 ```shell
		 my_dbt_project:
		   target: dev
		   outputs:
		     dev:
		       type: snowflake
		       account: <your_snowflake_account>
		       user: <your_snowflake_user>
		       password: <your_snowflake_password>
		       role: <your_snowflake_role>
		       database: <your_snowflake_database>
		       warehouse: <your_snowflake_warehouse>
		       schema: <your_snowflake_schema>
		       threads: 1
		       client_session_keep_alive: False
		 
		 ```
	- **Step 3: Use dbt with Astronomer and Docker**:
	
		1. Modify the Dockerfile: Navigate to your Astronomer project directory. You will modify the `Dockerfile` to include the installation commands for dbt and Soda. If the Dockerfile does not exist, the `astro dev init` command should generate it.
	 
	 	```shell
			# Use the official Astronomer Inc. Airflow image as a parent image
			FROM astronomerinc/ap-airflow:2.1.0-buster-onbuild

			# Install soda and dbt in separate virtual environments
			RUN python -m venv soda_venv && . soda_venv/bin/activate && \
			    pip install soda-core-snowflake==3.2.1 soda-core-scientific==3.2.1 pendulum && deactivate

			# Assuming you want dbt in its own environment
			RUN python -m venv dbt_env && . dbt_env/bin/activate && \
			    pip install dbt-snowflake && deactivate
	 		
	 	```
	 2. Configure profiles.yml: dbt uses a file named profiles.yml for connection configurations. This file is typically located at ~/.dbt/. Edit this file to include your Snowflake credentials:
	 
	 ```shell
	 my_dbt_project:
	   target: dev
	   outputs:
	     dev:
	       type: snowflake
	       account: <your_snowflake_account>
	       user: <your_snowflake_user>
	       password: <your_snowflake_password>
	       role: <your_snowflake_role>
	       database: <your_snowflake_database>
	       warehouse: <your_snowflake_warehouse>
	       schema: <your_snowflake_schema>
	       threads: 1
	       client_session_keep_alive: False
	 
	 ```	 
		 
#### 7. SODA installation and configuration

  - **Command to list** : `ls include/soda/` and `ls include/soda/checks/`
     
      ```shell
        ls include/soda/
	__pycache__  check_function.py  check_transform.py  checks  config.py  configuration_bill.yml  configuration_chart.yml  configuration_register.yml  configuration_transform.yml
	
	ls include/soda/checks/
	bill_tables  chart_tables  register_tables  transform
	
	ls include/soda/checks/register_tables/
	raw_address.yml  raw_date.yml  raw_location.yml  raw_user.yml
    ```
  - **configuration_register.yml_** : `ls include/soda/` 
     
        ```shell
	      data_source healthcare_db:
	       type: snowflake
	       username: ${SNOWFLAKE_USER}
	       password: ${SNOWFLAKE_PASSWORD}
	       account: ${SNOWFLAKE_ACCOUNT}
	       database: RAW 
	       warehouse: your warehouse
	       connection_timeout: 240
	       role: your role
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
	        warehouse: your warehouse
	        connection_timeout: 240
	        role: your role
	        client_session_keep_alive: true
	        authenticator: snowflake
	        session_params:
	          QUERY_TAG: soda-queries
	          QUOTED_IDENTIFIERS_IGNORE_CASE: false
	        schema: your schema
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
	           when required column missing: [UIDPK, UID, UFNAME, ULNAME, EMAIL, GendER, AGE, USERTYPE, UPDATE_AT]
	           when wrong column type:
	             UIDPK: NUMBER
	             UID: NUMBER
	             UFNAME: VARCHAR
	             ULNAME: VARCHAR
	             EMAIL: VARCHAR
	             GendER: VARCHAR
	             AGE: NUMBER
	             USERTYPE: VARCHAR
	             UPDATE_AT: TIMESTAMP_NTZ
				 ```
#### 8. Cosmos setup within Airflow: include/dbt/dbt_health/cosmos_config.py

- **cosmos_config**:
	```sql
	from cosmos.config import ProfileConfig, ProjectConfig
	from pathlib import Path
	DBT_CONFIG = ProfileConfig(
	profile_name='dbt_health',
	target_name='dev',
	profiles_yml_filepath=Path('/usr/local/airflow/include/dbt/dbt_health/profiles.yml')
	)
	DBT_PROJECT_CONFIG = ProjectConfig(
	dbt_project_path='/usr/local/airflow/include/dbt/dbt_health')

</details>

### 2. Design and Planning

<details>
<summary>Click to Expand: Data Modeling</summary>

- **Demonstrate Data Archetecture**
<br>
<img src="images/DataModeling.png" alt="header" style="width: 1100px; height: 500px;"><br>

---

</details>
<details>
<summary>Click to Expand: Data Quality Planning</summary>



#### 1. Data Quality (SodaCL) 


**Note**: Along with `data quality check` we should implement data observability. `Barr Moses CEO and CO-founder of Monte Carlo` coined "[Data observability](https://www.montecarlodata.com/blog-what-is-data-observability/)." She explaind that Data observability provides full visibility into the health of your data AND data systems so you are the first to know when the data is wrong, what broke, and how to fix it.
- **The five pillars of data observability:** 
  - Freshness
  - Quality 
  - Volume 
  - Schema
  - Lineage
    
#### 2. Data Quality Checks

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



---

### 3. Operational Tasks



<details>
 <summary>Click to Expand: Data Ingestion and Notification into Data Lake </summary>
 
#### Utilizing AWS S3 for data storage, Airflow for workflow automation, and Slack for notifications.

   - **Sources**: ingest data into `raw_files` folder (S3 buckets) and issue an `alert`.
     - **Folder Management and Notification**:
       - errors: error files stored in the `error_folder` with a `Slack alert`.
       - processed: processed data is ingested into snowflake with a`Slack alert`
  
   - Command to list S3 folders: `aws s3 ls s3://snowflake-emr`
     
       ```shell
         PRE error_files/
         PRE processed/
         PRE raw_files/
     ```
##### a. Naming Conventions:
  
   - Timestamps are used for file naming:
  
       ```python
         timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S')
       ```
##### a. Slack Notifications:
  
   - Slack webhook integration for notifications on success or failure: **Please ensure you've taken care of the security considerations (like not hardcoding AWS access keys or Slack Webhook URLs) when using these scripts in a real-world scenario. Use environment variables or secrets management tools instead**
  
       ```python
         SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/XXXXXXXXX/XXXXXXXXX/XXXXXXXXXXXXXX'  # Replace with your webhook URL
    
         def send_slack_message(message):
             #... [rest of the code]
       ```
       <br>
       <img src="images/slack.png" alt="header" style="width: 1110px; height: 500px;">
	   
##### Aiflow(Astro) dag
	
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
	
    <br>
    <img src="images/load_s3.png" alt="header" style="width: 1110px; height: 500px;">
	
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

- **Overview**: This DAG, `dynamic_s3_to_snowflake_etl`, is an advanced data pipeline designed for automating data flows from AWS S3 to Snowflake, and involves Slack for notifications. Let's review its tasks, workflow, and features:
  ```python
 
  	from datetime import datetime
	import pendulum
	from airflow.decorators import dag, task
	from airflow.models import Variable
	from airflow.providers.amazon.aws.hooks.s3 import S3Hook
	from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
	from airflow.operators.python import get_current_context
	
	from slack_sdk import WebClient
	from slack_sdk.errors import SlackApiError
	from airflow.operators.python import PythonOperator
	import json
	import logging
	from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
	
	# Configuration variables
	S3_BUCKET = Variable.get("S3_BUCKET")
	SNOWFLAKE_CONN_ID = "snowflake_default"
	SLACK_TOKEN = Variable.get("SLACK_TOKEN")
	SLACK_CHANNEL = Variable.get("SLACK_CHANNEL")
	SNOWFLAKE_STAGE_FULL_PATH = Variable.get("STAGE_NAME")
	SNOWFLAKE_TABLES = json.loads(Variable.get("SNOWFLAKE_TABLES", "[]"))
	S3_PREFIX = "raw_files/"
	S3_PROCESSED = "processed/"
	S3_ERROR = "error_files/"
	
	default_args = {
	    "owner": "airflow",
	    "depends_on_past": False,
	    "start_date": datetime(2021, 1, 1),
	}
	
	
	@dag(schedule="@daily", default_args=default_args, catchup=False, tags=["snowflake", "slack"])
	def dynamic_s3_to_snowflake_etl():
	    
	    is_file_available = S3KeySensor(
	        task_id='check_s3_for_file',
	        bucket_key=S3_PREFIX + '*.csv',  # Adjust the pattern to match your file naming
	        bucket_name=S3_BUCKET,
	        wildcard_match=True,
	        aws_conn_id='aws_default',
	    )
	
	
	    @task
	    def list_s3_files():
	        """List files in the specified S3 bucket and prefix."""
	        s3_hook = S3Hook(aws_conn_id="aws_default")
	        return s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX)
	
	    @task
	    def load_to_snowflake(key: str) -> dict:
	        """Load a file to Snowflake, returning the file name and load status."""
	        file_name = key.split('/')[-1].split('.')[0].upper()  # Extract file name and convert to uppercase
	        if file_name in [table.upper() for table in SNOWFLAKE_TABLES]:
	            try:
	                hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
	                copy_sql = f"""
	                    COPY INTO {file_name}
	                    FROM '@{SNOWFLAKE_STAGE_FULL_PATH}/{key.split('/')[-1]}'
	                    FILE_FORMAT = (FORMAT_NAME = 'MANAGE_DB.EXTERNAL_STAGES.MY_CSV_FORMAT');
	                """
	                hook.run(copy_sql, autocommit=True)
	                return {"file_name": file_name, "status": "SUCCESS"}
	            except Exception as e:
	                logging.error(f"Failed to load file {file_name} into Snowflake: {str(e)}")
	                return {"file_name": file_name, "status": "FAILURE"}
	        else:
	            return {"file_name": file_name, "status": "SKIPPED"}
	
	
	
	    @task
	    def notify_and_move_file(file_result: dict):
	        context = get_current_context()
	        execution_date = context['data_interval_start']  # Adjust based on your Airflow version
	        execution_date = pendulum.instance(execution_date)  # Ensure execution_date is a pendulum instance
	
	        file_name, status = file_result["file_name"], file_result["status"].lower()
	        s3_hook = S3Hook(aws_conn_id="aws_default")
	
	        # List files in the S3_PREFIX directory
	        all_files = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX)
	        # Find the exact filename by case-insensitive match
	        exact_filename = next((f for f in all_files if f.lower() == f"{S3_PREFIX}{file_name}.csv".lower()), None)
	
	        if exact_filename:
	            # Construct paths and keys with the exact filename
	            source_key = exact_filename
	            date_suffix = execution_date.strftime('%Y-%m-%d') + "/"
	            processed_path = f"{S3_PROCESSED}{date_suffix}{exact_filename.split('/')[-1]}"
	            error_path = f"{S3_ERROR}{date_suffix}{exact_filename.split('/')[-1]}"
	            destination_key = processed_path if status == "success" else error_path
	
	            # Attempt to move file
	            logging.info(f"Attempting to move file from {source_key} to {destination_key}")
	            try:
	                s3_hook.copy_object(source_bucket_key=source_key, dest_bucket_key=destination_key, source_bucket_name=S3_BUCKET, dest_bucket_name=S3_BUCKET)
	                s3_hook.delete_objects(bucket=S3_BUCKET, keys=[source_key])
	                logging.info(f"Moved {source_key} to {destination_key}")
	            except Exception as e:
	                logging.error(f"Failed to move file: {e}")
	
	            # Send notification to Slack
	            message = "successfully processed and moved to the processed folder." if status == "success" else "failed to load and moved to the error folder."
	            try:
	                client = WebClient(token=SLACK_TOKEN)
	                client.chat_postMessage(channel=SLACK_CHANNEL, text=f"File `{file_name}`: {message}")
	                logging.info(f"Notification sent to Slack for file {file_name}: {message}")
	            except SlackApiError as e:
	                logging.error(f"Failed to send message to Slack: {e.response['error']}")
	        else:
	            logging.error(f"Could not find exact file matching {file_name} in S3.")
	
	    # Define your DAG flow
	    s3_files = list_s3_files()
	    load_results = load_to_snowflake.expand(key=s3_files)
	    notify_and_move_files = notify_and_move_file.expand(file_result=load_results)
	    
	    is_file_available >> s3_files
	    
	
	dynamic_s3_to_snowflake_etl_dag = dynamic_s3_to_snowflake_etl()
  ```
  <br>
  <img src="images/snowflakeLoad.png" alt="header" style="width: 1110px; height: 500px;">
  
 #### Tasks:
  
   1. `check_s3_for_file (S3KeySensor)`: Monitors the S3 bucket for files matching a specific pattern, ensuring that the DAG proceeds only when the expected files are present. This sensor plays a crucial role in managing workflow execution based on data availability.
   2. `list_s3_files`: Lists all files in the specified S3 prefix, acting as the initial step to identify which files will be processed. This task is vital for dynamic file processing, accommodating varying numbers and names of files.
   3. `load_to_snowflake`: Processes each file from the list, attempting to load it into Snowflake. This task checks if the file name matches any table names specified in the Snowflake configuration, performs a COPY operation for matching files, and logs the outcome (SUCCESS, FAILURE, SKIPPED). This conditional loading is particularly useful for targeted data ingestion.
   4. `notify_and_move_file`: For each file processed, this task moves the file to a processed or error path based on the load outcome and sends a notification to Slack. It involves complex logic to accurately move files and report the status, showcasing the pipeline's ability to handle post-load management and communication.

 #### Workflow:
  
  - The DAG initiates by checking for the presence of files in S3.
  - Upon confirming file availability, it lists all files in the specified prefix.
  - For each file, the DAG attempts to load it into Snowflake, based on naming conventions that match Snowflake tables.
  - After attempting to load each file, it moves the file to an appropriate directory (processed or error) and notifies a Slack channel about the operation's result.

 #### Features:
 
  - **Dynamic File Handling**: The DAG is designed to dynamically handle multiple files, determining actions based on file names and processing outcomes. This flexibility is crucial for workflows dealing with variable data inputs.
  - **Integration with External Services**: Demonstrates robust integration with AWS S3 for data storage, Snowflake for data warehousing, and Slack for notifications, providing a comprehensive approach to data pipeline management.
  - **Error Handling and Notifications**: Includes sophisticated error handling mechanisms, such as moving files to an error directory and notifying team members via Slack, enhancing the pipeline's reliability and maintainability.
  - **Expandable Tasks**: Utilizes the .expand method for the `load_to_snowflake` and `notify_and_move_file` tasks, enabling parallel processing of multiple files. This feature optimizes performance and scalability.

</details>


<details>
<summary>Click to Expand: DBT Transform</summary>

####  Managing staging and transformation processes using DBT (SQL+Jinja) within the Airflow ecosystem, facilitated by Cosmos

- **Overview**: [dbt](https://www.astronomer.io/cosmos/) integrates seamlessly within the Airflow ecosystem to enable the efficient orchestration of dbt (data build tool) jobs via Airflow workflows. It allows users to schedule, monitor, and manage dbt tasks directly from Airflow, streamlining the data transformation process within their data pipelines.

- **Key Benefits**:
  - **Centralized Workflow Management**: Manage both dbt and Airflow tasks from a unified platform, enhancing coordination and visibility.
  - **Simplified Scheduling and Monitoring**: Utilize Airflow's scheduling capabilities to manage dbt runs, ensuring data models are updated timely.
  - **Error Handling and Alerts**: Benefit from Airflow's alerting mechanisms for prompt issue resolution in dbt runs.
  - **Scalability**: Meet growing data transformation needs with the scalable solutions provided by Cosmos and Airflow.
  - **Enhanced Collaboration**: Foster better collaboration across data teams by integrating dbt into Airflow workflows, facilitating seamless changes and insight sharing.

- **DBT Data Warehouse**
<br>
<img src="images/DBT-ARCHITECTURE.png" alt="header" style="width: 1230px; height: 600px;"><br>

---


- **DBT Environment**: This is my docker environment for DBT

		```bash
		
		stro@cfabfee5ced1:/usr/local/airflow$ ls -la include/dbt/dbt_health/
		total 32
		drwxr-xr-x  3 astro astro  96 Nov 30 20:39 analyses
		-rw-r--r--  1 astro astro 391 Feb 27 03:58 cosmos_config.py
		drwxr-xr-x  3 astro astro  96 Feb 29 23:40 dbt_packages
		-rw-r--r--  1 astro astro 475 Feb 28 09:27 dbt_project.yml
		drwxr-xr-x  3 astro astro  96 Nov 30 21:00 logs
		drwxr-xr-x  5 astro astro 160 Feb 28 03:06 macros
		drwxr-xr-x  4 astro astro 128 Feb 25 19:18 models
		-rw-r--r--  1 astro astro 109 Feb 14 02:00 package-lock.yml
		-rw-r--r--  1 astro astro  75 Feb 14 02:00 packages.yml
		-rw-r--r--  1 astro astro 497 Dec  1 15:05 profiles.yml
		drwxr-xr-x  3 astro astro  96 Nov 30 20:39 seeds
		drwxr-xr-x  3 astro astro  96 Nov 30 20:39 snapshots
		drwxr-xr-x 10 astro astro 320 Nov 30 21:17 target
		drwxr-xr-x  3 astro astro  96 Nov 30 20:39 tests
		
- **DBT Folder Description**

	- `analyses`: Stores analytical SQL queries. While not directly used for generating models, these queries can be helpful for exploring the data or performing complex analyses that don't fit into the model/transform paradigm.

	- `cosmos_config.py`: Likely a custom Python configuration file related to using Cosmos with your dbt project. It might contain settings or parameters for integrating dbt runs within the Airflow environment via Cosmos.

	- `dbt_packages`: Contains dbt packages installed as dependencies for the project. These packages can include reusable models, macros, and tests that can be integrated into your dbt project.

	- `dbt_project.yml`: The main configuration file for a dbt project. It defines project-level configurations, including the version of dbt required, model configurations, and where to look for models, tests, and snapshots.

	- `logs`: Stores log files generated by dbt runs. These can be useful for debugging or auditing purposes.

	- `macros`: Contains custom macros written in Jinja. Macros are reusable pieces of code that can extend dbt's functionality or encapsulate logic for use in multiple models or tests.

	- `models`: This is where the SQL queries for transforming data are stored. Models define the transformations that turn raw data into the structured form used for analysis.

	- `package-lock.yml & packages.yml`: These files manage dbt package dependencies. packages.yml defines which packages your project depends on, while package-lock.yml locks the versions of these packages for consistent environments.

	- `profiles.yml`: Specifies how dbt connects to your data warehouse. This file contains the connection credentials and other necessary parameters.

	- `seeds`: Contains CSV files that dbt can load directly into the data warehouse. Seeds are useful for small reference data.

	- `snapshots`: Stores configurations for dbt snapshots, which capture the state of a dataset at a point in time and can track changes to records over time.

	- `target`: The directory where dbt stores the artifacts it generates during a run, including compiled SQL code and documentation.

	- `tests`: Contains custom data tests written in SQL. dbt tests are used to ensure the data in your models meets specified validation criteria.


- **Best Practices**: Consistent naming conventions. For example:
	- src_[schema].yml
	- stg_[schema]__[table].sql
	- dim_/fct_[table].sql
	- rpt_[table].sql
	- Create a staging layer
	- Create a schema.yml file for each directory (or model)
	- Be explicit (new lines are cheap, brain power is expensive)
	- Align dbt directories with your database

	
- ***SQL+Jinja**: Example how to refactor `SQL` into `SQL+Jinja`

- SQL

	```sql
	SELECT
		transaction_ar_fk,
         sum(case when trans_type == 'charge' then amount end)AS trans_charge_amount,
         sum(case when trans_type == 'payment' then amount end) AS trans_payment_ amount_,
         sum(case when trans_type == 'adjustment' then amount end)AS trans_adjustment_amount,
		 sum( amount) AS total_amount
	FROM int_transaction_detail
	GROUP BY 1
	```
- Jinja

	```sql
	{% set trans_types = ['gross_charge','payment','adjustment'] %}
	SELECT
		transaction_ar_fk,
		{% for trans_type in trans_types%}
         sum(case when trans_type == '{{trans_type}}' then amount end)AS {{payment_methon}}_amount
		 {%endfor%}
		 sum(amount) AS total_amount
	FROM int_transaction_detail
	GROUP BY 1
	```
- **Sources vs Models**
	
	| Concept    | Purpose   													           |  FileType  |  Jinja Function   |
	|------------|--------------------------------------------------------------------------|------------|-------------------|
	| Source     | Pointers to raw data tables already loaded in your database.             | .yml       | {{source()}}      |
	| Model      | SQL scripts created in dbt that are compiled to build new tables/views.  | .sql       | {{ref()}}         |
	

- **Sources (CTE)**: Example of a source query

	```sql
	
	WITH  

	user AS (

		SELECT * FROM {{source('register','raw_user')}}
	),

	final as (

			SELECT
				UIDPK AS USER_PK,
				UID AS USER_ID,
				UFNAME AS FIRST_NAME,
				ULNAME AS LAST_NAME,
				EMAIL,
				GENDER,
				AGE,
				USERTYPE,
				UPDATE_AT 

			FROM user 

	)

	SELECT * FROM final
	
	```

- **Moldels (CTE)**: Example of a Data Mart query	

	```sql
	
		WITH

		    patient AS (

		        SELECT * FROM {{ref('stg_register__user')}}
		        WHERE USERTYPE = 'Patient'
		    ),

		     address AS (

		        SELECT * FROM {{ref('stg_register__address')}}
        
		    ),

		    final AS (

			SELECT
				p.USER_PK AS PATIENT_PK,
				p.USER_ID AS PATIENT_NUMBER,
				FIRST_NAME,
				LAST_NAME,
				EMAIL,
				GENDER,
				AGE,
		        STREET,
		        CITY,
		        STATE,
		        ZIP_CODE

			FROM patient p
		            LEFT JOIN address a
		                ON p.USER_PK = a.USER_ID

		)

		SELECT * FROM final
		
		
	```



- **Materialization**:

	
	| Type           | Output  | Description                                                                                                |
	|----------------|---------|------------------------------------------------------------------------------------------------------------|
	| View           | View      | Default setting. Creates a view based on the model SQL query. Fast to deploy but potentially slow to query. |
	| Table          | Table     | Creates a table and inserts data based on the model SQL query. Will rebuild and reload all data every time. Fast to query but potentially slow to deploy.   |
	| Increment      | Table     | Creates a table and inserts only newly updated data based on the model SQL query and specific column. Fast to query and deploy but more complex configuration.|
	| Ephemeral      | Common Table Expression (CTE) | No database object is created. The model SQL query will be wrapped in a CTE can be reused in othermodel queries. |
	
	
<br>
<img src="images/dbt_model.png" alt="header" style="width: 1100px; height: 500px;"><br>

---


- **Macros**: Consistent naming conventions. For example:

	- `Creating a Macro`:
	
	```sql
	{% macro cent_to_dollar(column_name) %}
	    {{ column_name }} / 100
	{% endmacro %}
	
	```
	
	
	- `Using a Macro`: The usage of {{ cent_to_dollar('column_name') }}
	
	```sql
	


	WITH adjustment AS (
	    SELECT * FROM {{ ref('stg_bill__adjustment') }}
	),
	grosscharge AS (
	    SELECT * FROM {{ ref('stg_bill__grosscharge') }}
	),
	payment AS (
	    SELECT * FROM {{ ref('stg_bill__payment') }}
	),
	consolidated_transactions AS (
	    SELECT adjustment.ADJUSTMENT_PK AS TRANSACTION_PK, adjustment.AR_FK AS TRANSACTION_ARFK FROM adjustment
	    UNION ALL
	    SELECT grosscharge.CHARGE_PK AS TRANSACTION_PK, grosscharge.AR_FK AS TRANSACTION_ARFK FROM grosscharge
	    UNION ALL
	    SELECT payment.PAYMENT_PK AS TRANSACTION_PK, payment.AR_FK AS TRANSACTION_ARFK FROM payment
	),

	transaction_detail AS (
	    SELECT
	        ct.TRANSACTION_PK, 
	        ct.TRANSACTION_ARFK,
	        g.cpt_units,
	        {{ cent_to_dollar('g.amount') AS gross_charge,
	        {{ cent_to_dollar('p.amount') }} AS payment,
	        {{ cent_to_dollar('a.amount') }} AS adjustment
	    FROM consolidated_transactions ct
	    LEFT JOIN adjustment a ON ct.TRANSACTION_ARFK = a.AR_FK
	    LEFT JOIN payment p ON ct.TRANSACTION_ARFK = p.AR_FK
	    LEFT JOIN grosscharge g ON ct.TRANSACTION_ARFK = g.AR_FK
	)
	SELECT * FROM transaction_detail
	
	```

- **Seed**: 

	1. `Step 1: Create a CSV File`: Open a spreadsheet program like Excel or Google Sheets, or a plain text editor, and create your CPT codes table. For example, your cpt_code.csv might start like this:
	```sql
	cpt_code,description
	99213,Office or other outpatient visit for the evaluation and management of an established patient
	99214,Office or other outpatient visit for the evaluation and management of an established patient, requiring at least two of these three key components
	99215,Office or other outpatient visit for the evaluation and management of an established patient, which requires at least 2 of these 3 key components
	
	```

	2. `Save the File`: Save this file as `cpt_code.csv`. Place it in your dbt project under the `seed` directory. If this directory does not exist, you should create it at the root of your dbt project:
	```sql
	your-dbt-project/
	├── seed/
	│   └── cpt_code.csv
	├── dbt_project.yml
	├── models/
	└── ...
	
	```
	3. `Step2: Run dbt seed`
		- Open Your Terminal: Navigate to the root of your dbt project.
		- Execute dbt Seed: Run the following command to load your CSV file into your data warehouse: `dbt seed`
		<br>
		<img src="images/seed.png" alt="header" style="width: 1100px; height: 500px;"><br>

		---
	4. Step3: Verify the Seed Data after running `dbt seed`
		<br>
		<img src="images/verify_seed.png" alt="header" style="width: 800px; height: 400px;"><br>
		0

- **Test and Documention**: Consistent naming conventions. For example:	

</details>

<details>
<summary>Click to Expand: Airflow Automate Transformation </summary>


### DAG: healthcare_db

- **Airflow Dag**:This Airflow DAG, healthcare_db, is designed for a healthcare data pipeline, integrating various tasks like data quality checks with Soda, data transformation with dbt, and data movement within a Snowflake environment. It demonstrates a complex, yet well-structured, approach to managing healthcare data workflows. 


```python
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging
from typing import List
from airflow.models.baseoperator import chain

from include.soda.config import external_python_config, task_configs

from include.dbt.dbt_health.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID")


try:
	REGISTER_TABLES = Variable.get("REGISTER_TABLES", deserialize_json=True)
	logging.info(f"REGISTER_TABLES: {REGISTER_TABLES}")
	except KeyError as e:
		logging.error(f"Error retrieving variable: {e}")
	    # Depending on your use case, you might set a default value or raise an exception
	    # For example, setting REGISTER_TABLES to an empty list if not found
	    REGISTER_TABLES = []
    
try:
	CHART_TABLES = Variable.get("CHART_TABLES", deserialize_json=True)
	logging.info(f"CHART_TABLES: {CHART_TABLES}")
	except KeyError as e:
	    logging.error(f"Error retrieving variable: {e}")
	    # Depending on your use case, you might set a default value or raise an exception
	    # For example, setting CHART_TABLES to an empty list if not found
	    CHART_TABLES = []   

try:
	BILL_TABLES = Variable.get("BILL_TABLES", deserialize_json=True)
	logging.info(f"BILL_TABLES: {BILL_TABLES}")
	except KeyError as e:
	    logging.error(f"Error retrieving variable: {e}")
	    # Depending on your use case, you might set a default value or raise an exception
	    # For example, setting BILL_TABLES to an empty list if not found
	    BILL_TABLES = []

def create_dbt_task_group(group_id: str, select_paths: list) -> DbtTaskGroup:
	return DbtTaskGroup(
	        group_id=group_id,
	        project_config=DBT_PROJECT_CONFIG,
	        profile_config=DBT_CONFIG,
	        render_config=RenderConfig(
	            load_method=LoadMode.DBT_LS,
	            select=select_paths
	        )
	    )


@dag(start_date=datetime(2023, 1, 1), schedule=None, catchup=False, tags=['healthcare'])
	def healthcare_db():
	    # Define tasks for performing quality checks using Soda for different table types
	    for table_type, config in task_configs.items():
	        @task.external_python(**external_python_config, task_id=f'check_load_{table_type}')
	        def check_load(scan_name: str, config_suffix: str, checks_subpath: str):
	            """Perform quality checks using Soda based on table type."""
	            from include.soda.check_function import check
	            return check(scan_name, config_suffix, checks_subpath)
      
	        globals()[f'check_load_{table_type}'] = check_load(**config)

   
@task
def truncate_and_insert(schema: str, tables: list):
	 """
	 Truncate and insert data into specified schema and tables with error handling.
	 """
	 hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
	 for table in tables:
	            try:
	                # Attempt to truncate table if exists
	                truncate_cmd = f"TRUNCATE TABLE IF EXISTS {schema}.{table};"
	                hook.run(truncate_cmd)
	                logging.info(f"Successfully truncated table: {schema}.{table}")
	            except Exception as e:
	                logging.error(f"Failed to truncate table {schema}.{table}: {str(e)}")

	            try:
	                # Attempt to insert data from PUBLIC schema to target schema
	                insert_cmd = f"INSERT INTO {schema}.{table} SELECT *,current_date() FROM data_raw.{table};"
	                hook.run(insert_cmd)
	                logging.info(f"Successfully inserted data into table: {schema}.{table}")
	            except Exception as e:
	                logging.error(f"Failed to insert data into table {schema}.{table}: {str(e)}")
                
                
@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
	    def check_transform(scan_name='check_transform', checks_subpath='transform'):
	        from include.soda.check_transform import inspector

	        return inspector(scan_name, checks_subpath)
    
	    check_transform = check_transform()

                

	    # DBT task groups for staging transformations
staging_register = create_dbt_task_group('stage_register', ['path:models/staging/register'])
staging_chart = create_dbt_task_group('stage_chart', ['path:models/staging/chart'])
staging_bill = create_dbt_task_group('stage_bill', ['path:models/staging/bill'])
    
	    # DBT task group for warehouse transformations
marts_warehouse = create_dbt_task_group('transform_warehouse', ['path:models/marts/warehouse'])
    
	    # Data movement tasks![Fileconfig](images/fileconfig.png)
move_data_register = truncate_and_insert(schema='register', tables=REGISTER_TABLES)
move_data_chart = truncate_and_insert(schema='chart', tables=CHART_TABLES)
move_data_bill = truncate_and_insert(schema='bill', tables=BILL_TABLES)
    
    
	    # Task dependencies for data movement and transformations using chain
chain(check_load_register, move_data_register, staging_register, check_transform, marts_warehouse)
chain(check_load_chart, move_data_chart, staging_chart, check_transform,  marts_warehouse)
chain(check_load_bill, move_data_bill, staging_bill, check_transform, marts_warehouse)

Instantiate the DAG
	    healthcare_db()

```	

#### Dag Graph pipeline: linage pipline
<br>
<img src="images/Dag_flow.png" alt="header" style="width: 1110px; height: 500px;">

#### Dag Graph pipeline: Stage Register 
<br>
<img src="images/Stage_register.png" alt="header" style="width: 1180px; height: 550px;">

#### Dag Graph pipeline: Data Models
<br>
<img src="images/Dag_datawarehouse.png" alt="header" style="width: 1150px; height: 550px;">


- **Tasks Overview**

	- `Quality Checks (Soda)`: Executes quality checks for different table types `(REGISTER_TABLES, CHART_TABLES, BILL_TABLES)` using Soda. It utilizes an external Python task to perform these checks based on predefined configurations.
	- `Truncate and Insert (Snowflake)`: For each specified schema and table list, this task truncates existing tables and inserts new data from a PUBLIC schema. It showcases error handling for both truncation and insertion operations.
	- `Check Transform (Soda)`: Performs additional transformation checks using Soda, specifically targeting the transformation processes to ensure data integrity post-transformation.
	- `DBT Task Groups for Staging Transformations`: Creates DBT task groups for staging transformations for register, chart, and bill data models. This modular approach allows for focused transformations within specific domains of the healthcare data model.
	- `DBT Task Group for Warehouse Transformations`: Similar to staging transformations, this task group focuses on transformations within the warehouse layer, preparing the data for analytical purposes.
	- `Data Movement Tasks`: These tasks move data for register, chart, and bill schemas by truncating existing tables and inserting new data. It's a critical step to refresh the data in preparation for transformation and analysis.

- **Features**

	- `Modular Task Groups`: Utilizes dbt task groups to modularly structure data transformations, improving maintainability and scalability.
	- `External Python Execution`: Employs external Python execution for Soda tasks, allowing for isolated environment management and dependency handling.
	- `Dynamic Task Creation`: Dynamically creates quality check tasks based on predefined configurations, demonstrating Airflow's flexibility in task management.
	- `Error Handling`: Implements robust error handling for data movement tasks, ensuring reliability and traceability of the data pipeline operations.
	- `Chain Dependencies`: Organizes task dependencies using chain for clarity and readability, streamlining the execution flow from quality checks to data movement and transformations.

</details>

---

### 4.  Report Task



<details>
<summary>Click to Expand: Clinical Finance (DBT) </summary>

#### Clinical Finance (DBT)
- **Common Report question for healthcare**:

1. How many rows of data are in the FactTable that include a Gross Charge greater than $100?
	```sql
    SELECT COUNT(*) CNT
    FROM FactTable
    WHERE GrossCharge > 100;
	```
	
2. How many unique patients exist is the Healthcare Database?
	```sql
    SELECT COUNT(distinct PatientNumber) as uniquePatients
    FROM dimPatient;
	```
	
3. How many CptCodes are in each CptGrouping?
	```sql
    SELECT CptGrouping, COUNT(DISTINCT CptCode) count_cpt
    FROM dimCptCode
    GROUP BY CptGrouping;
	```
	
4. How many physicians have submitted a Medicare insurance claim?
	```sql
    SELECT p.PayerName, COUNT(DISTINCT pn.ProviderNpi) AS ProviderCount
    FROM FactTable ft 
    JOIN dimPhysician pn ON ft.dimPhysicianPK = pn.dimPhysicianPK
    JOIN dimPayer p ON ft.dimPayerPK = p.dimPayerPK
    GROUP BY p.PayerName;
	
	```
	
5. Calculate the Gross Collection Rate (GCR) for each LocationName - See Below  GCR = Payments divided GrossCharge Which LocationName has the highest GCR?
	```sql
	WITH fact_data AS (

	  SELECT
	    l.Location_Name,
	    -SUM(ft.Payment) AS Payment,
	    SUM(ft.GrossCharge) AS GrossCharge
	  FROM {{ ref('FactTable') }} ft
	  JOIN {{ ref('dimLocation') }} l ON l.dimLocationPK = ft.dimLocationPK
	  GROUP BY l.Location_Name
	)

	SELECT
	  Location_Name,
	  TO_CHAR((Payment / NULLIF(GrossCharge, 0)), 'FM999999990.00%') AS GCR
	FROM fact_data
	ORDER BY GCR DESC
	```
	
6. How many CptCodes have more than 100 units?
	```sql
	WITH greater_count AS (
	    SELECT
	        cd.CptCode,
	        cd.CptDesc,
	        SUM(ft.CPTUnits) OVER (PARTITION BY cd.CptCode) AS count_cpt
	    FROM 
	        {{ ref('FCT_TRANSACTION') }} ft
	    JOIN 
	        {{ ref('dim_CptCode') }} cd 
	    ON 
	        ft.dimCPTCodePK = cd.dimCPTCodePK
	    GROUP BY
	        cd.CptCode, cd.CptDesc
	)

	SELECT 
	    CptCode,
	    CptDesc,
	    count_cpt
	FROM 
	    greater_count
	WHERE 
	    count_cpt > 100
	```
	
7. Find the physician specialty that has received the highest amount of payments. Then show the payments by month for this group of physicians. 
	```sql

	```
	
8. How many CptUnits by DiagnosisCodeGroup are assigned to a "J code" Diagnosis (these are diagnosis codes with the letter J in the code)?
	```sql
	{{ config(materialized='view') }}

	SELECT
	    dc.diagnosis_code,
	    SUM(ft.cpt_units) AS total_cpt_units
	FROM 
	    {{ ref('FCT_TRANSACTION') }} ft
	JOIN 
	    {{ ref('dim_diagnosiscode') }} dc 
	ON 
	    ft.diagnosis_code_pk = dc.diagnosis_code_pk
	WHERE 
	    dc.diagnosis_code LIKE 'J%'
	GROUP BY 
	    dc.diagnosis_code
	
	```
	
9. You've been asked to put together a report that details Patient demographics. The report should group patients into three buckets- Under 18, between 18-65, & over 65 Please include the following
	- columns:
		- First and Last name in the same column
		- Email
		- Patient Age
		- City and State in the same column
		```sql
		SELECT 
		    pt.FirstName || ' ' || pt.LastName AS "Name", 
		    pt.Email, 
		    pt.PatientAge AS "Age", 
		    pt.City || ', ' || pt.State AS "City and State",
		    CASE
		        WHEN pt.PatientAge < 18 THEN 'UNDER 18' 
		        WHEN pt.PatientAge BETWEEN 18 AND 65 THEN 'BETWEEN 18-65' 
		        WHEN pt.PatientAge > 65 THEN 'OVER 65' 
		    END AS "AGE RANGE"
		FROM 
		    {{ ref('dimPatient') }} pt;
	    
		```
		
10. How many dollars have been written off (adjustments) due to credentialing (AdjustmentReason)? Which location has the highest number of credentialing adjustments? How many physicians at this locatio have been impacted by credentialing adjustments? What does this mean?
	```sql
	```
	
11. What is the average patientage by gender for patients seen at Big Heart Community Hospital with a Diagnosis that included Type 2 diabetes? And how many Patients are included in that average?
	```sql
	```
	
12. There are a two visit types that you have been asked to compare (use CptDesc).
		- Office/outpatient visit est
		- Office/outpatient visit new
	Show each CptCode, CptDesc and the assocaited CptUnits.What is the Charge per CptUnit? (Reduce to two decimals) What does this mean? 
	```sql
	```
	
13. Similar to Question 12, you've been asked to analysis the PaymentperUnit (NOT ChargeperUnit). You've been tasked with finding the PaymentperUnit by PayerName. 
	Do this analysis the following visit type (CptDesc)
		- Initial hospital care
14. Within the FactTable we are able to see GrossCharges. You've been asked to find the NetCharge, which means Contractual adjustments need to be subtracted from the GrossCharge (GrossCharges - Contractual Adjustments).After you've found the NetCharge then calculate the Net Collection Rate (Payments/NetCharge) for each physician specialty. Which physician specialty has the 
	worst Net Collection Rate with a NetCharge greater than $25,000? What is happening here? Where are the other dollars and why aren't they being collected? What does this mean?
	```sql
	```
	
15. Build a Table that includes the following elements:
	- LocationName
	- CountofPhysicians
	- CountofPatients
	- GrossCharge
	- AverageChargeperPatients 

</details>

<details>
<summary>Click to Expand: Revenue Cycle(Jupyter Lab) </summary>

#### Revenue Cycle(Jupyter Lab)

- **Exploring Analysis**
	- [Data Exploration Notebook](https://github.com/Jayboy628/DataDrivenHealthcare/blob/main/revenue_cycle/exploring.ipynb)

- **Write off Analysis**
	- [Write-off Analysis Notebook](https://github.com/Jayboy628/DataDrivenHealthcare/blob/main/revenue_cycle/Writeoff_Analysis.ipynb)
	
- **Denial Analysis**
	- [Denial Analysis Notebook](https://github.com/Jayboy628/DataDrivenHealthcare/blob/main/revenue_cycle/Denial_Analysis.ipynb)
	
- **Relative Value Units (RVUs) Analysis**
	- [RVU Analysis Notebook](https://github.com/Jayboy628/DataDrivenHealthcare/blob/main/revenue_cycle/RVU_Analysis.ipynb)
	
</details>

<details>
<summary>Click to Expand: Clinical Dashboard (Tableau) </summary>


#### Clinical Dashboard (Tableau)

##### Explore our interactive Tableau dashboards for more in-depth analysis:

- **Clinical Demographics**
	- [Dashboard 1: Emergency Department Throughput](https://public.tableau.com/app/profile/shaunjay.brown/viz/EDThroughput_17084866657770/EDThroughput)

	- [Dashboard 2: Length of Stay](https://public.tableau.com/app/profile/shaunjay.brown/viz/LengthofStay_17080888461950/LengthofStay)

	- [Dashboard 3: Readmission Rate By Diagnosis and Demographics](https://public.tableau.com/app/profile/shaunjay.brown/viz/ReadmissionRate_17080887147200/Readmission)

- **Extra Dashboards**
- [Dashboard 1: Insurance](https://public.tableau.com/app/profile/shaunjay.brown/viz/InsuranceStatusofTotalAqusitionCost/InsuranceStatusofTotalAqusitionCost)

- [Dashboard 2: Sales By Region](https://public.tableau.com/app/profile/shaunjay.brown/viz/KPISalesbyRegion/KPI-SalesvsProfit)

</details>












