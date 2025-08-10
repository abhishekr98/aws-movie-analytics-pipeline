# AWS Movie Analytics Pipeline

## Overview
This project demonstrates an end-to-end AWS-based data engineering and analytics pipeline.  
We started with raw movie credits data in CSV format stored in S3, used AWS Glue for ETL (flattening JSON columns),  
and loaded the cleaned results back to S3 for analysis via Amazon Athena.

## Architecture
1. **S3** – Stores raw and cleaned datasets.
2. **AWS Glue** – PySpark scripts to transform and flatten the data.
3. **Athena** – Query the cleaned data using SQL.
4. **Optional** – Visualization using Power BI / Tableau.

## Workflow Steps
- Upload raw CSV to S3 bucket (`movieanalysis25/raw/`).
- Use Glue ETL job to flatten `cast` and `crew` JSON columns.
- Output cleaned CSV to `movieanalysis25/cleaned/`.
- Create Athena tables pointing to cleaned data.
- Run queries for analysis.

## Challenges & Solutions
- **Large CSV size** – Used Glue distributed processing.
- **Nested JSON columns** – Applied `from_json` and `explode` in PySpark.
- **Athena schema mismatches** – Ensured correct column data types.

## Skills Demonstrated
- AWS Glue ETL (PySpark)
- S3 Data Lake architecture
- SQL & Athena
- Data cleaning & transformation
- Cloud-based data analytics
