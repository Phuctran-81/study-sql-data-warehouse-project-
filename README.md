# Data Warehouse Project
This Data Warehouse project demonstrates building a comprehensive data warehouse to generating actionable insights.
---
## Data Architecture
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:
![Data Architecture](https://github.com/user-attachments/assets/c4a30b60-e527-48ee-bec1-e63dbe1c84e0)

1. **Bronze Layer:** Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer:** This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer:** Houses business-ready data modeled into a star schema required for reporting and analytics.
---
## Project Overview
This project involves:
1. **Data Architecture:** Designing a Modern Data Warehouse using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines:** Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling:** Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting:** Create SQL-based reports and dashboards for actionable insights.
### This repository includes:
- SQL Development
- Data Architect
- Data Engineering
- ETL Pipeline Developer
- Data Modeling
- Data Analytics
---
## Important Links & Tools
- **[Datasets](datasets/):** Access to the project dataset (csv files).
- **[Notion](https://www.notion.so/Data-Warehouse-Project-2ccabe17155980398c99fe9dffcdf0c4?source=copy_link):** Access to All Project Phases and Tasks.  
---
## Project Requirements
### Building the Data Warehouse (Data Engineering)
#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.
#### Specifications
- **Data Sources:** Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality:** Cleanse and resolve data quality issues prior to analysis.
- **Integration:** Combine both sources into a single, user-friendly data model designed for analitical queries.
- **Scope:** Focus on the latest dataset only; historization of data is not required.
- **Documentation:** Provide clear documentation of the data model to support both business stakeholders and analytics teams.
---

