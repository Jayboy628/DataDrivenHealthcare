<span style="color: red;">Still developing project below</span>


# <font color=blue><center>Overcoming EMR Challenges with Cloud-Based Solutions:</center></font>
### <font color=blue><center>Harnessing Cloud Technology for an Efficient Data Warehouse Solution</center></font>
I worked for a health company that encountered a major issue with their EMR system because it did not align with their business process. In turn, this caused the system to be bugy, as too many custom builts were implemented. The company decided to move away from their current system and instead implemented eClinicalWorks. The EMR company owned the database, so my company had to arrange an amendment to the contract that enables them to extend their usage agreement. The EMR company also agreed to FTP the live data files before work begins at 2:00 am and after work ends at 7:00 pm.  
My job was to design and implement a data warehouse from these files. The requirements included creating various production reports and KPI’s that matched with the EMR system. The business owners would compare eClinicalWorks integrated reports with my reports and if aligned, they would be flagged to be used for production. In the company’s view this was critical for data migration because it guaranteed that all operational reports would be correct and more importantly, would prove that eClinicalWorks was configured based on the company’s business requirements.  
My intention with this project is to replicate some of the more important aspects of the above scenario. Please note that the healthcare dataset is fake and is being used only for demonstration purposes. 

## <font color=green><left>PHASE ONE: Data Integration and Data Consolidation with Standardization </left></font>

<details open>
    
<summary>
    
### Extraction Approach: Apache Nifi
</summary>

<p>
The Ingestion (Apache Nifi) is designed to automate data across systems. In real time it will load (PutFile) the files into a local database (SQL Server) before pushing the files to the cloud storage(S3) environment
</p>

#### Table of Content
- NIFI: Goto http://localhost:8443/nifi/
    - NiFi Cofiguration
    - Push files using NiFi to Postges    
    - AWS: S3 Storage
        - Identity and Access Management (IAM)
        - Access Keys
        - Bucket
        - Folder
        - Upload Files
          
    <details open>
    
    <summary>
    
    #### 1) Nifi Configuration: Local Installation Setup 
    </summary>
    
        1) Nifi Configuration
          - Installing Nifi Toolkit
              export version='1.22.0'
              export nifi_registry_port='18443'
              export nifi_prd_port='8443'
  
          - Download Nifi Toolkit: I am using a MAC and my envrionment loaction is cd/opt
              - wget https://dlcdn.apache.org/nifi/${version}/nifi-toolkit-${version}-bin.zip cd /opt
              - unzip nifi-toolkit-${version}-bin.zip -d /opt/nifi-toolkit && cd  /opt/nifi-toolkit/nifi-toolkit-${version} &&  mv * .. && cd .. && rm -rf nifi-toolkit-${version}
          - Configuration Files
                  --- varibales loop ---
                  prop_replace () { target_file=${3:-${nifi_props_file}}
                  echo 'replacing target file ' ${target_file}
                  sed -i -e "s|^$1=.*$|$1=$2|"  ${target_file}}

                    mkdir -p /opt/nifi-toolkit/nifi-envs
                    cp /opt/nifi-toolkit/conf/cli.properties.example /opt/nifi-toolkit/nifi-envs/nifi-PRD
                    prop_replace baseUrl http://localhost:${nifi_prd_port} /opt/nifi-toolkit/nifi-envs/nifi-PRD
                    cp /opt/nifi-toolkit/conf/cli.properties.example /opt/nifi-toolkit/nifi-envs/registry-PRD
                    prop_replace baseUrl http://localhost:${nifi_registry_port} /opt/nifi-toolkit/nifi-envs/registry-PRD
  
        2) Ingest Files to Postgres Database
        3) Move Files to S3 bucket
    </details>
  <details open>
    
    <summary>
    
    #### 2) Ingest Files to Postgres Database
    </summary>
    
        1) Nifi Configuration
        2) Ingest Files to Postgres Database
        3) Move Files to S3 bucket
    </details>
  <details open>
    
    <summary>
    
     #### 3) Move Files to S3 bucket
    </summary>
    
        1) Nifi Configuration
        2) Ingest Files to Postgres Database
        3) Move Files to S3 bucket
    </details>
    
  
</details>


<details open>
    
<summary>
    
### Load Approach: Snowflake and SQL
</summary>

<p>
 The next step is to populate the cloud database. Snowpipe will pull the normalized Json files from AWS into tables. As previously stated, the agreement with the EMR company was to FTP the files twice a day. 
    I would be required to configure the load by creating a Task (Acron) and a Stream (CDC). This would enable triggers for a scheduled load and would continuously update the appropriate tables.
</p>

- Snowflake: Database
    - Data Warehouse and SQS Setup
        - Database and Schema
            - Table
                - Type-1
                - Type-2
            - View
                - DBT (explained in next section)
            - Stored procedure
            - Snow Pipe
            - Stream
            - Task

</details>

## <font color=green><left>PHASE TWO: Reporting and Analytics </left></font>
<details open>
    
<summary>
    
### Transformation, Documentation: DBT and SQL
</summary>

<p>
 Another requirement was implementing a Data Warehouse that enabled the stakeholders to view and compare the reports and KPIs. Since Data Warehouse usage is mainly for analytical purposes rather than transactional, I decided to design a Star Schema because the structure is less complex and provides better query performance. Documenting wasn’t required, however, adding the Data Build Tool (DBT) to this process allowed us to document each dimension, columns and visualize the Star Schema. DBT also allowed us to neatly organize all data transformations into discrete models.  
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

<details open>
    
<summary>
    
### Analyze Approach: Language of choice Python and Tableau
</summary>

<p>
My intention with this project is to replicate some of the more important aspects of the above scenario. <font color=red>Please note that the healthcare dataset is fake and is being used only for demonstration purposes. </font>
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

## <font color=green><left>PHASE: THREE </left></font>
* Models


