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
### Agenda

- Cloud-Based Solutions: Healthcare Data Warehouse
  - Ingestion Approach
    - [Installing Nifi Toolkit & Nifi](https://nifi.apache.org/docs/nifi-docs/html/getting-started.html): Setup Nifi Environment
    - Automate Log parsing
    - Nifi Ingest Data to Staging Database (PostgreSQL)
    - Nifi Automate PostgreSQL Database to Store JSON File in AWS (S3)
  - Load Approach
    - Implementing dedicated virtual warehouse
    - CREATE PERMISSION FOR ROLES (SQL)
    - Cloud Storage (S3)
    - Data Warehouse 
  - Reporting Approach
    - CMS 


#### <font color="green"><left>PHASE ONE: Data Ingestion, Data Storage, Data Warehouse Layers</left></font>
---------------------------------------------------------------------------------------------------------------------

<details>
<summary>

##### Cloud Technology: [Apache Nifi](https://nifi.apache.org/), [Slack](https://slack.com/),[S3](https://aws.amazon.com/) and [PostgreSQL](https://www.postgresql.org/)

</summary>

### Ingestion Approach
-----------------------
The Ingestion (Apache Nifi) is designed to automate data across systems. In real-time, it will load (PutFile) the files into a local database (Postgres) before pushing the files to the cloud storage (S3) environment.<br><br>
The next step is to populate the cloud database. Snowpipe will pull the normalized JSON files from AWS into tables. As previously stated, the agreement with the EMR company was to FTP the files twice a day. I would be required to configure the load by creating a Task (Acron) and a Stream (CDC). This would enable triggers for a scheduled load and would continuously update the appropriate tables.<br><br>

#### Diagram Shows Ingestion Approach

<img src="images/main2.png" alt="header" style="width: 900px; height: 400px;"><br>

<details>
<summary>
    
##### 1) Goto [NIFI](http:/localhost:8443/nifi/): Setup Nifi Environment
</summary>

- Setup Nifi Environment: (I am using a MAC)
  - Open Terminal
  - Move to the following folder: `cd /opt`
- Installing Nifi Toolkit: You can download the Apache Nifi [here](https://nifi.apache.org/download.html) or follow these steps:
  - Create the following variables:
    - `export version='1.22.0'`
    - `export nifi_registry_port='18443'` (I am keeping the illustration simple. However, install registry, prod, dev stg is recommended)
    - `export nifi_prd_port='8443'`
  - Download Nifi Toolkit: I am using a MAC and my environment location is `cd/opt`
    - `wget https://dlcdn.apache.org/nifi/${version}/nifi-toolkit-${version}-bin.zip cd /opt`
    - `unzip nifi-toolkit-${version}-bin.zip -d /opt/nifi-toolkit && cd /opt/nifi-toolkit/nifi-toolkit-${version} && mv * .. && cd .. && rm -rf nifi-toolkit-${version}`
  - Configuration Files
  
    Using the variables created above to configure Loop
    ----------------------------------------------------
    
    ```shell
    prop_replace () {
      target_file=${3:-${nifi_props_file}}
      echo 'replacing target file ' ${target_file}
      sed -i -e "s|^$1=.*$|$1=$2|" ${target_file}
    }

    mkdir -p /opt/nifi-toolkit/nifi-envs
    cp /opt/nifi-toolkit/conf/cli.properties.example /opt/nifi-toolkit/nifi-envs/nifi-PRD
    prop_replace baseUrl http://localhost:${nifi_prd_port} /opt/nifi-toolkit/nifi-envs/nifi-PRD
    cp /opt/nifi-toolkit/conf/cli.properties.example /opt/nifi-toolkit/nifi-envs/registry-PRD
    prop_replace baseUrl http://localhost:${nifi_registry_port} /opt/nifi-toolkit/nifi-envs/registry-PRD
    ```
    
    ### NIFI CLI STEPS:
    
    <strong>The config files have the following properties</strong>
    -----------------------------------------------------------------------------
    
    - Configure this nifi-PRD
      - Type the following: `cd /opt/nifi-toolkit/nifi-envs`
      - Add the following to `baseUrl`: `baseUrl=http://localhost:8443` 
    - Type the following and enter Nifi Toolkit env: `/opt/nifi-toolkit/bin/cli.sh`
    - Show Session Keys: `session keys`
    - Add session: `session set nifi.props /opt/nifi-toolkit/nifi-envs/nifi-DEV`

    <strong>View the nifi Environment</strong>
    ---------------------------------------------------------------
     
    - Start Nifi: `/opt/nifi-prd/bin/nifi.sh start` 
    - Start Nifi-toolkit: `/opt/nifi-toolkit/bin/cli.sh`                 `
    - View current Session: `session show`
    - Find the root PG Id: `nifi get-root-id`
    - List all Process Groups: `nifi pg-list` (its empty,but will be used in `Files to Postgres Database` section)
    - Find the current user: `nifi current-user`
    - List all available templates: `nifi list-templates` (its empty, haven't add any template as yet)

     <strong>Below is a basic view of Nifi Environment</strong>
    ---------------------------------------------------------------
     
    <img src="images/fileconfig.png" alt="header" style="width: 1000px; height: 700px;"><br> 

</details>


<details>
<summary>
  
##### 2) Goto [NIFI](http:/localhost:8443/nifi/): Automate Log parsing
</summary>

<strong> Setup Log parsing inside NIFI</strong>
---------------------------------------------------------------

- Log file location: `/opt/nifi-prd/logs` we can view the log files `nifi-app.log`
- Start Nifi: `/opt/nifi-prd/bin/nifi.sh start` 
- Start Nifi-toolkit: `/opt/nifi-toolkit/bin/cli.sh`
- Goto your nifi web location: `http:/localhost:8443/nifi/`
    - Drag Process Group icon onto the plane and name it `Healthcare Data Process` then double click to open another plane
    - Drag another `Process Group` and name it `LOGS`

<strong> Create the Log Flow in Nifi</strong>
---------------------------------------------------------------

- Drag the `Processor` onto the plane and type `TailFile` and Relationship is success
- Open the TailFaile Configure page and click on the `SETTINGS` and click on `Bulletin Level`
    - Will mirror the flow base on the `Bulletin Level` Then click on `PROPERTIES`
    - In `Property` column  `Tailing mode` choose Value `Single file` and in column `File(s) to Tail` add the log path
    - ***Log file Path**: `/opt/nifi-prd/logs/nifi-app.log`<br><br>

    - TailFile Configure Processor: `Bulltin Level`
    ------------------------------------------
    <img src="images/Bulletin.png" alt="header" style="width: 700px; height: 400px;"> <br>

    - TailFile Configure Processor: `PROPERTIES`
    ------------------------------------------
    <img src="images/TailFile.png" alt="header" style="width: 700px; height: 500px;"> <br>

    - Connect `TailFile` RELATIONSHIPS to Success `SplitText`
    - Configure Processor for `SplitText`: Line Split Count `1`this split the `Bulltin Level type`
        - ***Header Line Count***: `0`
        - ***Removing Trailing Newlines***: `True`
    - Connect `SplitText` RELATIONSHIPS to Success `RouteOnContent` and Terminate: `failure` and `original`
    - Configure Processor for `RouteOnContent`
        - ***Match Requirement***: `content must contain match`
        - ***Character Set***: `UTF`
        - ***Content Buffer Size*** : `1 MB`
        - ***Click*** the `+` and manually add the following:
            - DEBUG : connect to LongAttribute
            - ERROR : connect to `ExtractGrok`
            - INFO : connect to LongAttribute
            - WARN : connect to LongAttribute
            - See Below <br>
                - <img src="images/AddBulltin.png" alt="header" style="width: 600px; height: 400px;"> <br>
    - Connect `RouteOnContent` RELATIONSHIPS to Success `ExtractGrok` and Terminate: `unmatched`
    - Configure Processor for `ExtractGrok`
        - ***Grok Expression***: `%{TIMESTAMP_ISO8601:timestamp} %{LOGLEVEL:level} \[%{DATA:thread}\] %{DATA:class} %{GREEDYDATA:message}`
        - ***Character Set***: `flowfile-attribute`

    - If you have a `Slack` account Connect `RouteOnContent` RELATIONSHIPS to Success `PutSlack`
    - Configure Processor for `RouteOnContent`
        - ***Webhook URL***: `Sensitive value set`
        - ***Webhook Text***: ` An Error occoured at ${grok.timestamp} with Service ${grok.thread}. Error msg ${grok.message}`
        - Channel: <Your slack Channel>

    NIFI: LOG DATA FLOW
    ------------------------------------------
    <img src="images/logfile.png" alt="header" style="width: 700px; height: 500px;"> <br>   
            
 
</details>

  <details>
<summary>
  
 ##### 3) Goto [NIFI](http:/localhost:8443/nifi/): Ingest Data to Staging Database (PostgreSQL)
</summary>
    
- Incorporating a staging database may seem like an unnecessary step since the files are already standardized. However, there are several benefits to consider. Firstly, it provides cost-effectiveness. Utilizing the cloud for repeated SELECT operations can be expensive. Secondly, the staging database allows for the identification of any unforeseen data issues and enables additional data cleansing and standardization processes. The ultimate goal is to minimize the number of updates and inserts into Snowflake, ensuring optimal efficiency.
- ***FTP LOCATION***: I used python script to create a `timestamp` and `increment count` for each file.
  - `Python Script`:[Script](code): I also implement `Slack` to notify me that the file reachs `2:AM Before work and 7:PM `
  - To integrate the Incoming `Webhooks` feature into the code, you'll need to make the following modifications:
    1. Install the slack_sdk library if you haven't already: `pip install slack_sdk`
    2. Import the necessary modules: `from slack_sdk import WebClient`,`from slack_sdk.errors import SlackApiError`
    3. Set up the Slack webhook URL: `slack_webhook_url = 'YOUR_SLACK_WEBHOOK_URL'`: Click here to view script [Script](code)

- Automate configuration file within parameter-context 
    - ***Create two folders***: Process-Nifi and parameter_context
    - /opt/nifi-toolkit/nifi-envs/`Process-Nifi/parameter_context` and add the files [`postgres-config.json`](parameter-context) to the folder
    - ***Start Nifi-toolkit***: `/opt/nifi-toolkit/bin/cli.sh`
    - ***Create the parameter Context for database***:
    `nifi import-param-context -i /opt/nifi-toolkit/nifi-envs/Excel-NiFi/parameter_context/postgres-config.json' -u http://localhost:8443`
    - ***Create the parameter Context for file Tracker***:
    `nifi import-param-context -i /opt/nifi-toolkit/nifi-envs/Excel-NiFi/parameter_context/excell-healthcare-tracker-config.json' -u http://localhost:8443`
    - ***Goto your nifi web location***: `http:/localhost:8443/nifi/`
    - ***Open Nifi***: In the top right corner click the icon and click on `Parameter Contexts` to confirm that the above files are loaded
    - *** Global Gear***: Click on it and search in the `Process Group Parameter Context` for your loaded files and click apply
        - Drag Process Group icon onto the plane and name it `Healthcare Data Process` then double click to open another plane
        - Drag another `Process Group` and name it `File Extraction to Databases`
            - Click the process group `File Extraction to Database` and then Drag the Processor and type `List File`
                - In the ListFile processor the file configuration should be loaded inplace automatically
                - ***Input Directory*** : `#{source_directory}`
                - ***File Filter*** : `#{file_list}`
                - ***Entity Tracking Node Identifier*** : `${hostname()}`

            - Drag the Processor and type `FetchFile`
                - ***File to Fetch*** : `${absolute.path}/${filename}`
                - ***Move Conflict Strategy*** : `Rename`
            
            - Drag the Processor and type `ConvertRecord`: Read CSV files and convert to `JSON`
                - ***Record Reader*** :`CSVReader`: we needed configure a `Controller Service Details` click on `properties`
                    - ***Schema Access Strategys*** : `Infer Schema`
                    - ***CSV Parse*** : `Apache Commons CSV`
                    - ***CSV Format*** : `Microsoft Excel`
                - ***Record Writer*** : `JsonRecordSetWriter`
                    - ***Schema Write Strategy*** : `Set 'avro.schema' Attribute`
                    - ***Schema Access Strategy*** : `Inherit Record Schema`
                    - ***Output Grouping*** : `Array`
                    - ***Compression Format*** : `None`

            - Drag the Processor and type `ConvertJSONToSQL`: Read JSON files and convert to `SQL Queries`
                - ***JDBC Connection Pool*** :`JPostgreSQL-DBCPConnectionPool`: we needed configure a `Controller Service Details` click on `properties`

                - NIFI upload JSON config file for Database: `JPostgreSQL-DBCPConnectionPool`
                -----------------------------------------------------------------------------
                <img src="images/DBCPConnectionConfig.png" alt="header" style="width: 700px; height: 400px;"> <br>
                
                - ***Statement Type*** : `INSERT`
                - ***File Filter*** : `#{filename:replace('.csv')}`
              

            - Drag the Processor and type `PUTSQL`: Read JSON files and convert to `SQL Queries INSERT`
                - ***JDBC Connection Pool*** :`JPostgreSQL-DBCPConnectionPool`: we needed configure a `Controller Service Details` click on `properties`
                - ***Batch Size*** : `1000`
                - ***Rollback On Failure*** : `true`

               - NIFI Data Flow `Set up scheduled or event-driven processes to load data from NiFi into PostgreSQL`
                -----------------------------------------------------------------------------
                <img src="images/File_Database.png" alt="header" style="width: 700px; height: 800px;"> <br>
                
</details>

  <details>
<summary>
  
 ##### 4) Goto [NIFI](http:/localhost:8443/nifi/): Automate PostgreSQL Database to Store JSON File in AWS (S3)
</summary>
    
- ***Staging Database (PostgreSQL)***: The staging database acts as an intermediary storage area where the raw data from the ingestion layer is initially stored. It provides a temporary storage location for data cleansing, validation, and transformation processes.
***Cloud Storage (S3)***: The cloud storage, such as Amazon S3, is used to store the processed and transformed data. It provides scalable and cost-effective storage for large volumes of data, ensuring durability and availability.
- ***Data Transformation and Staging***: A Guide below but`Beyond the scope of the project`
    - Install and configure PostgreSQL database on a dedicated server or cluster
    - Create the necessary tables and schemas in PostgreSQL to stage the incoming data
    - Design SQL scripts or stored procedures to perform data transformation, standardization, and cleansing based on specific business rules
    - Implement data validation and quality checks to ensure the integrity of the staged data
    - Set up scheduled or event-driven processes to load data from `NiFi PostgreSQL to Storage (S3)`.
-AWS S3 CONFIGURATION
----------------------------------------
- **Create a User**:
    - Login to the `AWS Management Console`
    - In the search bar, type `IAM` and click on `IAM (Identity and Access Management)`
    - Click on `Users` from the left-hand menu and then click on `Add User`
    - Enter a name for the user and select `Programmatic access` for the `Access type`
    - Click on `Next: Permissions` and then select `Attach existing policies directly`
    - Search for and select the `AmazonS3FullAccess` policy
    - Click on `Next: Tags` (optional) and then click on `Next: Review`
    - Review the user details and click on `Create user`
    - Take note of the `Access key ID` and `Secret access key` as you will need them in the Nifi configuration

- **Create an S3 Bucket**:
    - Go to the `AWS Management Console`
    - In the search bar, type `S3` and click on `S3`
    - Click on `Create bucket`
    - Enter a unique name for the bucket and choose the region
    - Click on `Next` and leave the rest of the settings as default
    - Click on `Next` and review the bucket settings
    - Click on `Create bucket`

 - ***Start Nifi***: `/opt/nifi-prd/bin/nifi.sh start`
 - ***Goto your nifi web location***: `http:/localhost:8443/nifi/`
    - Drag another `Process Group` and name it `Database Extraction to AWS(S3)`
    - Click the process group `Database Extraction to AWS(S3)` and then Drag the Processor and type `ExecuteSQL`
    - In the `ExecuteSQL processor` we need to query the tables
        - ***Database Connection Pooling Service*** : `PostgreSQL-DBCPConnectionPool`
            - ***SQL select query*** : `SELECT * FROM CHARGES` -> `We can make this more dynamic however, Its beyond the Scope`
        - Drag the Processor and type `ConvertRecord`: follow the previous config 
        - Drag the Processor and type `UpdateAttribute`: Reads the table names
            - ***Click `+` and name `filename`*** :`${sql.tablename}.json`: returns json file
        - Drag the Processor and type `PutS3Object`: sends the file to Storage (S3)
            - ***Object Key***: `${filename}`
            - ***Bucket*** : The Name you gave your `S3 Storage`
            - ***Access Key ID*** : `Sensitive value set`
            - ***Secret Access key*** :  `Sensitive value set`
            - ***Storage Class*** : `Standard`
            - ***Region*** : `Where your AWS Account is located`

              - ***Stage Database***: `PostgreSQL Database`-> Click [Here](https://github.com/Jayboy628/DataDrivenHealthcare/blob/main/code/Load.ipynb) To View
            -----------------------------------------------------------------------------
            <img src="images/Stage_Database1.png" alt="header" style="width: 1200px; height: 600px;"> <br>
                

              - ***NIFI Data Flow***: `PostgreSQL Database`
                -----------------------------------------------------------------------------
                <img src="images/Database_S3.png" alt="header" style="width: 700px; height: 800px;"> <br>
                
              - ***AWS Storage***: `S3`
                -----------------------------------------------------------------------------
                <img src="images/Storage_S3.png" alt="header" style="width: 600px; height: 400px;"> <br>


</details>


<details>
    
<summary>


##### Cloud Technology: [S3](https://aws.amazon.com/) and [Snowflake](https://www.snowflake.com/en/) (SQL)

</summary>

##### 5) Goto [Snowflake](https://app.snowflake.com/): AWS (S3) to Snowflake

### Load Approach
-----------------------

<p>
The next step is to populate the cloud database. Snowpipe will pull the normalized JSON files from AWS into tables. As previously stated, the agreement with the EMR company was to FTP the files twice a day. I would be required to configure the load by creating a Task (Acron) and a Stream (CDC). This would enable triggers for a scheduled load and would continuously update the appropriate tables.
</p>

- Implementing dedicated virtual warehouse

<table>
<tr> 
    <th><h5>CREATE DATA WAREHOUSE (SQL)</h5></th>
</tr>
<tr>
<td>  
<pre lang="js">
USE ROLE ACCOUNTADMIN;

    CREATE WAREHOUSE HEALTHCARE_WH 
    WITH WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1 
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'This is  a Data Warehouse for Healthcare';
</pre>
</td>
</tr>
</table>
<table>
<tr> 
    <th><h5>CREATE PERMISSION FOR ROLES (SQL)</h5></th>
</tr>
<tr>
<td>  
<pre lang="js">

  - ***CREATE ROLE FOR TRANSFOMATION***:`CREATE ROLE TRANSFORM_ROLE;`
  - ***GRANT PRIV SYSADMIN***: `GRANT MODIFY ON WAREHOUSE HEALTHCARE_WH TO ROLE ACCOUNTADMIN;`    
  - ***Create Databases (SQL)***: `CREATE DATABASE HEALTHCARE_RAW;` AND `CREATE DATABASE HEALTHCARE_DEV;` AND `CREATE DATABASE HEALTHCARE_PROD;`
  - ***MODFIY DATABASE PRIV (SQL)***: `GRANT MODIFY ON DATABASE HEALTHCARE_RAW TO ROLE TRANSFORM_ROLE;` AND `GRANT MODIFY ON DATABASE HEALTHCARE_DEV TO ROLE TRANSFORM_ROLE;`
 AND `GRANT MODIFY ON DATABASE HEALTHCARE_PROD TO ROLE TRANSFORM_ROLE;`
  - ***GRANT PRIVALEGE ON RAW DATABASE FOR SCHEMA,TABLES AND VIEWS(SQL)***: `GRANT USAGE ON DATABASE HEALTHCARE_RAW TO ROLE TRANSFORM_ROLE;` AND `GRANT USAGE ON DATABASE HEALTHCARE_DEV TO ROLE TRANSFORM_ROLE;` AND `GRANT USAGE ON DATABASE HEALTHCARE_PROD TO ROLE TRANSFORM_ROLE;`
  - ***GRANT PRIVALEGE ON HEALTHCARE_RAW DATABASE FOR SCHEMA,TABLES AND VIEWS(SQL)***:
`GRANT CREATE SCHEMA ON DATABASE HEALTHCARE_RAW TO ROLE TRANSFORM_ROLE;` AND `GRANT MODIFY ON DATABASE HEALTHCARE_RAW TO ROLE TRANSFORM_ROLE;` AND `GRANT MODIFY ON ALL SCHEMAS IN DATABASE HEALTHCARE_RAW TO ROLE TRANSFORM_ROLE;`
 - ***GRANT USAGE ON DATABASE HEALTHCARE_RAW TO ROLE TRANSFORM_ROLE;*** `GRANT USAGE ON ALL SCHEMAS IN DATABASE HEALTHCARE_RAW TO ROLE TRANSFORM_ROLE;`AND `GRANT SELECT ON ALL TABLES IN DATABASE HEALTHCARE_RAW TO ROLE TRANSFORM_ROLE;` AND `GRANT SELECT ON ALL VIEWS IN DATABASE HEALTHCARE_RAW TO ROLE TRANSFORM_ROLE;`
 - ***CREATE SCHEMA***: `CREATE SCHEMA HEALTHCARE_RAW.EMR;`

</pre>
</td>
</tr>
</table>
<table>
<tr> 
    <th><h5>CREATE TABLES (SQL)</h5></th>
</tr>
<tr>
<td>  
<pre lang="js">

  - ***CREATE TABLE EMR.patient(***
    -  patientPK varchar(255)	Not Null
    - ,PatientNumber varchar(255)	NULL
    - ,FirstName varchar(255)	NULL
    - ,LastName varchar(255) NULL
    - ,Email varchar(255)	NULL
    - ,PatientGender varchar(255)	NULL
    - ,PatientAge int	NULL
    - ,City varchar(255) NULL
    - ,State varchar(255)		NULL);

  - ***CREATE TABLE EMR.doctor(***
	   - doctorPK varchar(255)	Not NULL 
	   - ,ProviderNpi varchar(255)	NULL
	   - ,ProviderName varchar(255) NULL
	   - ,ProviderSpecialty varchar(255)	NULL
	   - ,ProviderFTE decimal(10,2)	NULL Default 0);

  - ***CREATE TABLE EMR.charge(***
    - chargePK varchar(255)	Not NULL
    - ,TransactionType varchar(255)	NULL
    - ,Transaction varchar(255)	NULL
    - ,AdjustmentReason varchar(255) NULL);

  - ***CREATE TABLE EMR.payer(***
	 - payerPK varchar(255)	Not NULL
	 - ,PayerName varchar(255)	NULL ); 
    
  - ***CREATE TABLE EMR.location(***
	 - locationPK varchar(255)	Not NULL 
	 - ,LocationName varchar(255) NULL);

  - ***CREATE TABLE EMR.diagnosis(***
	 - CodePK varchar(255)	Not NULL 
	 - ,DiagnosisCode varchar(255)	NULL
	 - ,DiagnosisCodeDescription varchar(255) 	NULL
	 - ,DiagnosisCodeGroup varchar(255) NULL);
    
    CREATE TABLE EMR.Code(
	  CodePK varchar(255)				Not NULL
	,CptCode varchar(255)				NULL
	,CptDesc varchar(255)				NULL
	,CptGrouping varchar(255)			NULL
    );

  - ***CREATE TABLE EMR.Date(***
    - PostPK varchar(255) Not NULL 
    - ,Date Date	NULL
    - ,Year varchar(255) NULL
    - ,Month varchar(255)	NULL
    - ,MonthPeriod varchar(255) NULL
    - ,MonthYear varchar(255)	NULL
    - ,Day varchar(255)	NULL
    - ,DayName varchar(255)	NULL);
  </pre>
</td>
</tr>
</table>

 - ***CREATE USER***:
```shell
    CREATE USER TRANSFORM_USER
    PASSWORD = 'yourpassword'
    LOGIN_NAME = 'LOGIN_NAME'
    DEFAULT_ROLE='TRANSFORM_ROLE'
    DEFAULT_WAREHOUSE = 'HEALTHCARE_WH'
    MUST_CHANGE_PASSWORD = FALSE;
    GRANT ROLE TRANSFORM_ROLE TO USER TRANSFORM_USER;
```

</details>

</details> 

#### <font color="green"><left>PHASE TWO: Data Transformation, Documentation, Data Visualization and Reporting Layers</left></font>
---------------------------------------------------------------------------------------------------------------------

<details>
    
<summary>

##### Cloud Technology: [DBT](https://www.getdbt.com/)(***SQL***)

</summary>

##### 6) Goto [DBT](https://auth.cloud.getdbt.com/login): Snowflake and DBT 

### Transformation, Documentation Approach
------------------------------------------
<p>
Another requirement was implementing a Data Warehouse that enabled the stakeholders to view and compare the reports and KPIs. Since Data Warehouse usage is mainly for analytical purposes rather than transactional, I decided to design a Star Schema because the structure is less complex and provides better query performance. Documenting wasn’t required, however, adding the Data Build Tool (DBT) to this process allowed us to document each dimension, columns, and visualize the Star Schema. DBT also allowed us to neatly organize all data transformations into discrete models.
</p>

- DBT: Documentation and Transformation
  - Tables
    - Dimensions
    - Facts
    - SCD
      - Type-1
      - Type-2
    - build operational reports (push to BI Tool)
      
</details>

<details>
    
<summary><strong><em> Analyze Approach: Language of choice Python and Tableau</em></strong></summary>

<p>
My intention with this project is to replicate some of the more important aspects of the above scenario. <font color="red">Please note that the healthcare dataset is fake and is being used only for demonstration purposes.</font>
</p>

- Jupyter Lab
  - Data Exploring
  - Data Cleansing
  - Recycle Revenue Reports
- Tableau Healthcare Reports
  - Revenue Reports
  - PMI Reports
  - CMS Reports

</details>

#### <font color="green"><left>PHASE THREE</left></font>
---------------------------------------------------------------------------------------------------------------------

* Models
