# <font color=blue><center>Data Migration for Healthcare Industry </center></font>
I worked for a health company that encountered a major issue with their EMR system. The EMR system, NextGen, did not align with our business process and in turn caused the system to be bugy. This is because of too many custom builds implemented. The company decided to move away from NextGen and implemented a new EMR system called eClinicalWorks. NextGen owned the database, so we had to make an agreement to extend our off time while periodically they would FTP our data via files. 

My job was to create and implement data warehouse from these files sent by NextGen. The requirements included creating various production reports and KPI’s that matched with NextGen. The business owners would compare eClinicalWorks integrated reports with my reports and if aligned then flagged to be used for production. In the company’s view this was critical for data migration because it guarantees that all operational reports were correct. 

My intention with this project is to replicate some of the more important aspects of the above scenario. <font color=red>Please note that the healthcare dataset is fake and is being used only for demonstration purposes. </font>

## <font color=green><left>PHASE: ONE </left></font>

<details open>
    
<summary>
    
### Extraction:
</summary>

<p>
    My intention with this project is to replicate some of the more important aspects of the above scenario. <font color=red>Please note that the healthcare dataset is fake and is being used only for demonstration purposes. </font>
</p>

- Goto http://localhost:2080/nifi/
- NiFi-S3 integration
- Push files using NiFi
- Organize and Storage
- S3
- Identity and Access Management (IAM)
- Access Keys
- Bucket
- Folder
- Upload Files
  
</details>

<details open>
    
<summary>
    
### Load:
</summary>

<p>
My intention with this project is to replicate some of the more important aspects of the above scenario. <font color=red>Please note that the healthcare dataset is fake and is being used only for demonstration purposes. </font>
</p>

- Data Warehouse and SQS Setup
- Snowflake
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
    
### Transformation, Documentation:
</summary>

<p>
My intention with this project is to replicate some of the more important aspects of the above scenario. <font color=red>Please note that the healthcare dataset is fake and is being used only for demonstration purposes. </font>
</p>

- DBT: Language of choice SQL
- Dimensions
- Facts
- SCD
- Type-1
- Type-2
- Reports
- Basic Reports connect to BI tool (eClinicalWorks uses Cognos)
  
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
- Healthcare Reports
- Revenue Reports (can use BI tools)
- PMI Reports (can use BI tools) 
- CMS Reports (can use BI tools)

</details>

* Models

### Nifi
Apache NiFi is a data logistics platform that automates data transfer across systems. It gives real-time control over data transportation from any source to any destination, making it simple to handle.%                  
