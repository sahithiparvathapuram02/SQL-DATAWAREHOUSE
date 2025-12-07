# SQL-DATAWAREHOUSE

**PROJECT TITLE**

SQL-DATAWAREHOUSE

**📌PURPOSE**

This project implements a modern Data Warehouse architecture using the Bronze–Silver–Gold layered approach.
The goal of the system is to ingest raw data from multiple sources, clean and transform it using SQL, and provide trusted data models for analytics, BI dashboards, and reporting.

**🗂️ DATA SOURCES**

The project uses the following databases:

source_crm

|Table|Description|
|-----|-----------|
|cust_info|cst_id,	cst_key,	cst_firstname,	cst_lastname,	cst_marital_status,	cst_gndr,	cst_create_date|
|prd_info|property_id, property_name, prd_id,	prd_key	prd_nm,	prd_cost,	prd_line,	prd_start_dt,	prd_end_dt|
|dim_rooms|sls_ord_num,	sls_prd_key,	sls_cust_id,	sls_order_dt,	sls_ship_dt	sls_due_dt,	sls_sales	sls_quantity,	sls_price|

source_erp

|Table|Description|
|-----|-----------|
|cust_az12|CID,	BDATE,	GEN|
|loc_a101|CID, CNTRY|
|px_cat_g1v2|ID, CAT,	SUBCAT,	MAINTENANCE|


**🏗️ ARCHITECTURE LAYERS**

**🥉 1. Bronze Layer – Raw Data**

The Bronze layer stores raw, unprocessed, and historical data exactly as received from source systems.

**Purpose**

  * Acts as a secure backup of source systems

  * Ensures data lineage and auditability

  * Stores unmodified data for replay or reprocessing

**Characteristics**

  * No data cleansing

  * No transformations

  * Includes duplicates, nulls, and errors

  * Partitioned by load date (optional)

**Examples**

  * Raw CSV, JSON, Excel data

  * Transaction logs

  * Extracts from ERP/CRM

  * API/raw event streams

**🥈 2. Silver Layer – Cleansed & Transformed Data**

The Silver layer contains clean, standardized, and business-ready tables created from Bronze data.

**Purpose**

  * Clean data for usability

  * Apply transformations

  * Enforce data types and schema

  * Build conforming dimensions

  * Remove duplicates and invalid entries

**Transformations in Silver**

  * Data type conversions

  * Deduplication

  * Missing value handling

  * Data standardization (dates, names, formats)

**🥇 3. Gold Layer – Business & Analytics Layer**

The Gold layer contains final, curated data models optimized for reporting and analytics.

**Purpose**

  * Serve Power BI dashboards

  * Provide trusted facts and dimensions

  * Join transformed tables into star or snowflake schema

  * Create business metrics and KPIs

**Outputs**

  * Star Schema Models

  * Aggregated tables

  * KPI models

  * Data marts for Finance, Sales, Marketing, etc.

**Examples**

  * fact_sales

  * dim_customers

  * dim_products

**📊 TYPICAL DATA FLOW**
```
Source Systems
      ↓     
   Bronze (Raw Layer)
      ↓ Cleaning, Validation      
   Silver (Refined Layer)   
      ↓ Modeling, Aggregation    
   Gold (Business Layer)
      ↓ 
   Power BI / Analytics / Dashboards
```
**🧰 TECHNOLOGIES USED**

* SQL Server 

* ETL Tools: Stored Procedures

* Power BI for dashboarding

* Stored Procedures, Views for automation


**🔄 ETL / ELT PROCESS**

1. Extract

  * Load raw files into Bronze

  * No transformations performed

2. Transform

  * Clean and standardize data in Silver

  * Apply business logic

3. Load

  * Load into Gold fact and dimension tables

  * Build star schema

  * Publish dataset for BI tools


**🧪 QUALITY & VALIDATION**

  * Duplicate checks

  * Null checks

  * Referential integrity validation

  * Metadata and audit logs

  * Error handling pipelines

**📊 REPORTS DEVELOPED**

As part of this project, the following analytical reports were designed and developed using the curated Gold–Layer datasets:

1️. Customer Report

2. Product Report


**📁 FOLDER STRUCTURE**

```
  /SQL-DataWarehouse-Project
│
├── /Datasets
│   ├── source_crm
│   └── source_erp
|
├── /Bronze
│   ├── bronze_ddl.sql
│   └── bronze_load.sql
│
├── /Silver
│   ├── silver_ddl.sql
│   └── datachecks.sql
|   └── silver_load.sql
│
├── /Gold
│   ├── gold_ddl.sql
│   └── quality_check.sql
|
├── /Reports
│   ├── CustomerReport.sql
│   └── ProductReport.sql
```

**📝 CONCLUSION**

This project follows the industry-standard Medallion Architecture (Bronze/Silver/Gold) to ensure:

  * Data quality

  * Traceability

  * Reusability

  * Performance

  * Scalability

It is designed to support analytics, reporting, and machine learning by offering clean, reliable, and business-ready data.
