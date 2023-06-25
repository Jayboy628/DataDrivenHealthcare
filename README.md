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
1) The Ingestion (Apache Nifi) is designed to automate data across systems. In real time it will load (PutFile) the files into a local database (SQL Server) before pushing the files to the cloud storage(S3) environment. . See diagram below: 
</p>

- NIFI: Ingest
    - Goto http://localhost:2080/nifi/
        - NiFi-S3 integration
        - Push files using NiFi
        - Organize and Storage
          
- AWS: Storage
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
2) The next step is to populate the cloud database. Snowpipe will pull the normalized Json files from AWS into tables. As previously stated, the agreement with the EMR company was to FTP the files twice a day. 
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
3) Another requirement was implementing a Data Warehouse that enabled the stakeholders to view and compare the reports and KPIs. Since Data Warehouse usage is mainly for analytical purposes rather than transactional, I decided to design a Star Schema because the structure is less complex and provides better query performance. Documenting wasn’t required, however, adding the Data Build Tool (DBT) to this process allowed us to document each dimension, columns and visualize the Star Schema. DBT also allowed us to neatly organize all data transformations into discrete models.  
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


