# <font color=blue><center>Data Migration for Healthcare Industry </center></font>
I worked for a health company that encountered a major issue with their EMR system. The EMR system, NextGen, did not align with our business process and in turn caused the system to be bugy. This is because of too many custom builds were implemented. The company decided to move away from NextGen and implemented a new EMR system called eClinicalWorks. NextGen owned the database, so we had to make an agreement to extend our off time while 2:AM and 7:PM they would FTP our data files. 

My job was to create and implement data warehouse from these files sent by NextGen. The requirements included creating various production reports and KPI’s that matched with NextGen. The business owners would compare eClinicalWorks integrated reports with my reports and if aligned then flagged to be used for production. In the company’s view this was critical for data migration because it guarantees that all operational reports were correct. 

My intention with this project is to replicate some of the more important aspects of the above scenario. <font color=red>Please note that the healthcare dataset is fake and is being used only for demonstration purposes. </font>

## <font color=green><left>PHASE: ONE </left></font>

<details open>
    
<summary>
    
### Extraction: Nifi, SqlServer, AWS
</summary>

<p>
1) The Extraction (Nifi) design was simply to pull the data files (FTP) in a dev environment and then use SQL Server Integration Service (SSIS) to Load and Transform. Instead, for this illustration I opted to implement a cloud load environment. However, since the files are normalized, I would load (PutFile) the files into a local database (SQL Server) before pushing the file to the cloud storage(S3) environment. . See diagram below: 
</p>

- NIFI
    - Goto http://localhost:2080/nifi/
    - NiFi-S3 integration
    - Push files using NiFi
    - Organize and Storage
- AWS
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

- Snowflake:Data Warehouse and SQS Setup
    - Warehouse/Virtual Warehouse
    - Database and Schema
        - Table
        - View
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

- DBT: Language of choice SQL
    - Dimensions
    - Facts
    - SCD
    - Type-1
    - Type-2
    - build operational reports (push to BI Tool)
  
</details>

## <font color=green><left>PHASE: THREE </left></font>
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

* Models

### Nifi
Apache NiFi is a data logistics platform that automates data transfer across systems. It gives real-time control over data transportation from any source to any destination, making it simple to handle.%                  
