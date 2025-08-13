# AWS Data Engineering Project – Movie Analytics Pipeline

**Role:** Azure Databricks Analyst exploring AWS Data Engineering  
**Goal:** End-to-end data pipeline design and analytics workflow using AWS services for a real-world dataset.  
**Duration:** ~2 Days  
**Outcome:** Fully automated data ingestion, transformation, and querying pipeline on AWS.

---

## 1. Background
As an experienced **Azure Databricks & Analytics Specialist**, I initiated this project to broaden my cloud engineering expertise into AWS.  
The challenge was to **ingest, clean, transform, and analyze a large raw dataset (181 MB+)** using AWS-native tools, replicating an enterprise-level ETL workflow.  

The dataset used was **Movie Credits and Metadata**, stored in Amazon S3.

---

## 2. Architecture Overview
**AWS Services Used:**
- **Amazon S3** – Data storage for raw and cleaned datasets  
- **AWS Glue** – Serverless ETL and data transformation  
- **AWS Athena** – Serverless SQL querying  
- **AWS IAM** – Role & policy management for secure service access  

**Pipeline Flow:**
1. **Raw Data Upload** → S3 (`movieanalysis25/raw/`)
2. **ETL with AWS Glue** → Flatten nested JSON/CSV structures, standardize schema
3. **Output Cleaned Data** → S3 (`movieanalysis25/cleaned/`)
4. **Data Cataloging** → Glue Data Catalog for Athena
5. **Data Querying** → Athena for SQL-based exploration & insights

---

## 3. Implementation Steps

### Step 1 – Data Ingestion
- Uploaded multiple CSV/JSON datasets into `raw/` folder in S3.
- Configured S3 bucket policies for Glue and Athena read/write.

### Step 2 – Data Transformation with AWS Glue
- Wrote custom **PySpark Glue ETL scripts** to:
  - **Flatten nested structures** (`cast` & `crew` JSON columns in credits file)
  - Standardize column names and data types
  - Handle null values and missing records
- Optimized transformation using **DynamicFrames** for schema flexibility.

**Example Output Structure – `cast_data`**
| id | cast_id | character | credit_id | gender | name | order_num |

### Step 3 – Data Storage
- Stored cleaned, structured datasets into `cleaned/` folder in S3.
- Verified file size reduction (~Spark compression), ensuring no data loss.

### Step 4 – Athena Table Creation
- Used **CREATE EXTERNAL TABLE** statements with OpenCSVSerde to define schema for each dataset.
- Linked Athena to Glue Data Catalog for schema persistence.
- Handled type mismatch errors by adjusting schema (e.g., parsing numeric columns as STRING where necessary).

### Step 5 – Data Analysis
- Ran exploratory SQL queries in Athena, such as:
  - **Top 10 most featured actors**
  - **Movies with largest cast**
  - **Cross-join with crew data for director-actor collaborations**
- Achieved **serverless querying** without provisioning infrastructure.

---

## 4. Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| Session conflicts in Glue Interactive Sessions | Cleared previous sessions via AWS Glue Console before re-execution |
| Schema mismatches (HIVE_BAD_DATA errors) | Adjusted Athena table definitions to match CSV field types |
| Flattening large nested JSON arrays | Used `explode()` in PySpark to create row-level entries |
| Cost visibility delay | Monitored **AWS Cost Explorer** after execution for accurate billing |

---

## 5. Key Learnings
- Transitioning Azure Databricks knowledge to AWS Glue was smooth due to **similar Spark foundations**.
- **Athena + Glue Data Catalog** offers a fast, low-maintenance querying layer.
- Efficient schema design in Glue saves hours of debugging in Athena.
- S3’s folder-based partitioning plays a huge role in query performance.

---

## 6. Business Impact (Hypothetical)
If applied in production for a streaming service:
- **Automated data refresh** from raw ingestion to final reporting.
- **Faster analytics** enabling daily cast/crew trend tracking.
- **Scalable architecture** supporting TB-scale datasets with minimal maintenance.

---

## 7. Tech Stack
- **Languages:** Python (PySpark), SQL
- **AWS Services:** S3, Glue, Athena, IAM
- **Data Processing:** AWS Glue DynamicFrames, Spark transformations
- **Data Querying:** Athena SQL with Glue Data Catalog

---

## 8. Conclusion
This project demonstrates **end-to-end AWS Data Engineering capability** — from ingestion, transformation, and schema management to analytics.  
It reflects my ability to **adapt existing Azure Databricks expertise to AWS services** while following best practices for **scalability, governance, and cost efficiency**.
