# <font color=blue><center>Data Migration for Healthcare Industry </center></font>
I worked for a health company that encountered a major issue with their EMR migration. Their previous EMR system, NextGen, was not a good solution for their business requirements, so they decided to migrate their health records to eClinicalWorks. My job was to create and implement a data warehouse, and from the data warehouse design reports, KPIs and build predictive models. The business owners would compare eClinicalWorks integrated reports with my reports. This was done in a dev environment and when the reports matched the eClinicalWorks reports it was then flagged to be used for production.  

My intention with this project is to replicate some of the more important aspects of the above scenario. <font color=red>Please note that the healthcare dataset is fake and is being used only for demonstration purposes. </font>

## <font color=green><left>AGENDA</left></font>
* Extraction
    - Goto http://localhost:2080/nifi/
    - NiFi-S3 integration
    - Push files using NiFi
* Organize and Storage
    - S3
        - Identity and Access Management (IAM)
        - Access Keys
        - Bucket
        - Folder
        - Upload Files
* Data Warehouse and SQS Setup
    - Snowflake
        - Warehouse/Virtual Warehouse
        - Database and Schema
        - Table
        - View
        - Stored procedure
        - Snow Pipe
        - Stream
        - Task
* Transformation, Load and Documentation
    - DBT: Language of choice SQL
        - Dimensions
        - Facts
        - SCD
        - Type-1
        - Type-2
        - Reports
            - Basic Reports connect to BI tool (eClinicalWorks uses Cognos)

* Analyze: Language of choice Python
    - Jupyter Lab
        - Data Exploring
        - Data Cleansing
        - Healthcare Reports
            - Revenue Reports (can use BI tools)
            - PMI Reports (can use BI tools) 
            - CMS Reports (can use BI tools)      
* Models

### Nifi
Apache NiFi is a data logistics platform that automates data transfer across systems. It gives real-time control over data transportation from any source to any destination, making it simple to handle.%                  
