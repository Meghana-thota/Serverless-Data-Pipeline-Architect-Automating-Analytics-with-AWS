# 🏪 Serverless Event-Driven Retail Data Pipeline  

This project demonstrates an **end-to-end retail data pipeline** that automates ingestion, transformation, and processing using AWS services and Apache Kafka.

---

# Retail Analytics — End-to-End (Local & AWS)

An end-to-end retail analytics project built twice:
1) **Local (VS Code + PySpark + Plotly)** and  
2) **Cloud (AWS S3 + Glue + Athena + QuickSight)**,  
with Bronze/Silver/Gold modeling, quality gates, and optional **S3 → Lambda → Glue** automation.

## Highlights
- **Data model:** Bronze → Silver → Gold pattern
- **Data quality:** quarantine + fail-fast threshold
- **Cloud analytics:** Athena SQL + QuickSight visuals (KPI, trends)
- **Automation:** S3 object upload triggers a Glue job via Lambda
- **Local analytics:** Plotly dashboard exported to HTML

---

## Tech Stack
- **Pandas**
- **Numpy**
- **Seabron**
- **ETL**
- **Python (PySpark)**
- **Apache Kafka**
- **AWS S3, Lambda, Glue**
- **Serverless Event-Driven Architecture**
- **Git / GitHub**

---

## Steps to Run
1. **Upload Raw Data** to S3 bucket (`raw/` folder).  
2. **Lambda Trigger** activates AWS Glue job.  
3. **Glue Job** runs PySpark ETL script on the incoming data.  
4. **Processed Output** stored in `processed/` S3 folder.  

---

##  Screenshots
<img width="1702" height="640" alt="image" src="https://github.com/user-attachments/assets/0683e295-9e15-42e4-9b76-cb8718269db8" />

<img width="1679" height="791" alt="image" src="https://github.com/user-attachments/assets/f8a2ea3d-0a5c-4083-ac4e-cf9e3e84a6f3" />

<img width="1067" height="861" alt="image" src="https://github.com/user-attachments/assets/cddb01de-8b42-436c-b3e6-c7b7dcffc147" />



---


## Repos & Code Map

- `01-local-pyspark-superstore/`
  - `src/etl_clean_spark.py` — cleans & aggregates (Silver/Gold)
  - `src/plotly_dashboard.ipynb` — rich visuals → `reports/retail_dashboard.html`
- `02-aws-glue-athena-quicksight/`
  - `glue/etl_clean_spark_glue.py` — Glue job script we ran successfully
  - `athena/ddl.sql` and `athena/queries.sql` — database/tables + sample queries
  - `quicksight/dashboard-notes.md` — visuals, fields, calculations
- `03-s3-lambda-glue-trigger/`
  - `lambda/handler.py` — listens to S3 PUT → starts Glue job with params
- `04-kafka-pyspark-streaming/`
  - `src/stream_etl.py` — Kafka → Structured Streaming → Bronze/Silver/Gold

---

## 👥 Collaborators
- [@Meghana-thota](https://github.com/Meghana-thota)
- (Add your friend’s GitHub username once added as collaborator)

---

## 📈 Future Enhancements
- Integrate **AWS Athena** for query analysis.  
- Add **QuickSight Dashboard** for visualization.  
- Extend pipeline for **real-time analytics** using Kinesis.

---

## 🏁 Conclusion
A fully automated, scalable, and serverless **Retail Data Pipeline** powered by **Kafka and AWS** — showcasing real-time data processing for modern data engineering use cases.
