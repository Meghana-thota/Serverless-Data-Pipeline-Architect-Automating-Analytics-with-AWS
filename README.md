# Retail Analytics ETL Pipeline (AWS + PySpark)  
**Production-ready serverless pipeline for retail data**  
  
This repository contains the end-to-end implementation of a retail data analytics ETL pipeline built using AWS serverless services (S3, Lambda, Glue, Athena, QuickSight). The source dataset is the “Global Superstore” retail data from Kaggle, covering customer orders, product categories, sales & profits.  

Using a layered architecture (Bronze→Silver→Gold), we develop locally with PySpark and then deploy to AWS: raw CSV files in S3 trigger Lambda → Glue ETL → processed data in S3 → queried via Athena → visualised with QuickSight.  
  
---

## Project Goals  
- Demonstrate how to take a real-world business dataset and build a **scalable**, **automated**, **cloud-native** data engineering pipeline.  
- Build logic locally (VS Code, PySpark) for cleaning, transforming and aggregating monthly revenue summaries to validate before cloud deployment.  
- Implement a fully serverless workflow: **any new file upload** triggers the pipeline end-to-end without manual steps.  
- Produce analytics-ready data layers (Silver, Gold) and deliver visualization KPIs/trends to business stakeholders.  

---

## Architecture Overview  

| Stage | Description |
|-------|-------------|
| **Local Development** | Use Python + PySpark scripts in VS Code: read raw CSVs → clean & dedupe → aggregate → output Parquet summaries. |
| **AWS Ingestion & Automation** | Raw CSV files uploaded to S3 “raw” bucket trigger an S3 event. That event invokes a Lambda function which starts a Glue ETL job. |
| **Data Lake Layers** | Within AWS: Bronze = raw CSV in S3; Silver = cleaned Parquet in S3; Gold = aggregated monthly revenue summaries in S3. |
| **Query & Visualisation** | Use Athena to query the Gold layer in S3; connect to QuickSight to build dashboards showing KPIs (total sales, profit margin, region/category breakdowns, trend over time). |
| **Serverless Automation** | No servers to manage — S3, Lambda, Glue, Athena and QuickSight all managed services; new data triggers the full workflow automatically. |

---

## Tech Stack  
- Local: Python, PySpark, VS Code  
- Cloud: AWS S3, AWS Lambda, AWS Glue (PySpark job), AWS Athena, Amazon QuickSight  
- Storage format: Parquet for intermediate/processed layers  
- Data source: Kaggle “Global Superstore” retail dataset  

---

## Setup & Usage  

### Local Development  
```bash
# Clone the repo
git clone https://github.com/<your-username>/retail-analytics-etl-aws.git
cd retail-analytics-etl-aws

# Create a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run local ETL layers
python src/etl_raw_spark.py       # loads raw CSV to bronze format
python src/etl_clean_spark.py     # cleans & dedupes → silver
python src/etl_aggregate_spark.py # computes monthly revenue summaries → gold
```

AWS Deployment

- Create three S3 buckets (or prefix folders) for: raw, clean/silver, gold data.
- Configure an S3 bucket event notification: when a new CSV lands in the raw zone, invoke the Lambda function.
- Lambda function logic: pass file path to Glue job.
- Create a Glue job (PySpark) with script glue_etl_job.py.
- Job reads raw files → transforms → writes silver & gold layers to respective S3 zones.
- Logs run metadata (job name, start/finish time, row counts) for auditing.
- Define Athena database/tables pointing to S3 gold layer.
- In QuickSight, connect to the Athena tables and build dashboards:
- KPI: Total Sales, Total Profit, Profit Margin
- Trend: Monthly revenue over time, by region, by product category
- Breakdown: Sales by region, category share, top customers

**Results & Business Impact**

Automates ingestion of retail CSVs and transforms them into analytics-ready monthly summaries.
Reduces manual effort: once set up, new data triggers the full workflow automatically.
Enables near-real-time business visibility into sales trends, profit margins, region/category performance.
Demonstrates architecture for scaling: as data volume grows, Parquet + serverless services scale effectively without infrastructure overhead.

**Future Enhancements**

Add streaming ingestion via Apache Kafka (or AWS Kinesis) for real-time/near-real-time data flows.
Introduce orchestration/scheduling via Apache Airflow or AWS Step Functions to manage dependencies, retries, and monitoring.
Infrastructure as Code: define AWS resources using Terraform for repeatable, versioned deployments.
Add data quality checks/monitoring (row counts, null rates, schema drift) and alerting (via CloudWatch/SNS).
Expand datasets and analytics: e.g., customer lifetime value (CLV), churn analysis, forecasting using ML.
