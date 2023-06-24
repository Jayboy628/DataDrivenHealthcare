# <font color=blue><center>Data Migration for Healthcare Industry</center></font>
I worked for a health company that encountered a major issue with their EMR system. The EMR system, NextGen, did not align with our business process and in turn caused the system to be bugy. This is because of too many custom builds were implemented. The company decided to move away from NextGen and implemented a new EMR system called eClinicalWorks. NextGen owned the database, so we had to make an agreement to extend our off time while 2:AM and 7:PM they would FTP our data files. 

My job was to create and implement data warehouse from these files sent by NextGen. The requirements included creating various production reports and KPI’s that matched with NextGen. The business owners would compare eClinicalWorks integrated reports with my reports and if aligned then flagged to be used for production. In the company’s view this was critical for data migration because it guarantees that all operational reports were correct. 

My intention with this project is to replicate some of the more important aspects of the above scenario. <font color=red>Please note that the healthcare dataset is fake and is being used only for demonstration purposes. </font>

## <font color=green><left>PHASE: ONE </left></font>

<title>InsightByte/ApacheNifi: Youtube Apache NiFi 2022 Series resources</title>


<details open>
    
<summary>
    
### Extraction Approach: Apache Nifi
</summary>

<p>
1) The Ingestion (Apache Nifi) is design to automate data across systems. In realtime I would load (PutFile) the files into a local database (SQL Server) before pushing the file to the cloud storage(S3) environment. . See diagram below: 
</p>

- NIFI: Click the link to view configuration
    - Goto http://localhost:2080/nifi/
        - NiFi-S3 integration
        - Push files using NiFi
        - Organize and Storage
          
- AWS: Click the link to view configuration
    - S3
        - Identity and Access Management (IAM)
        - Access Keys
        - Bucket
        - Folder
        - Upload Files
  
</details>

<details open>
    
<summary>
    
### Load: Snowflake and SQL
</summary>

<p>
2) The next step is to populate the cloud database. Snowpipe will pull the normalized Json files from AWS into tables. I would need to create a Task (Acron)remember; the files will be sent twice a day so I will need to scheduler also I need to build a Stream (CDC) to enable triggers.
</p>

- Snowflake:Click the link to view configuration
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

## <font color=green><left>PHASE: TWO </left></font>
<details open>
    
<summary>
    
### Transformation, Documentation: DBT and SQL
</summary>

<p>
3) For us to develop reports and KPI’s I needed to design a star schema. I wanted to document each Dimension and their columns and visualize the schema. DBT is a great tool for this process because DBT doesn’t only work well with Snowflake but it’s awesome tool to create and visualize the star schema while documenting. 
</p>

- DBT: Click the link to view configuration (Language of choice SQL)
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
    
### Analyze: Language of choice Python and Tableau
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


