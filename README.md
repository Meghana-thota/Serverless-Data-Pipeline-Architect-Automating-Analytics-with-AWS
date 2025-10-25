# 🏪 Serverless Event-Driven Retail Data Pipeline  
### Using Apache Kafka, AWS S3, Lambda, and Glue (PySpark ETL)

This project demonstrates an **end-to-end retail data pipeline** that automates ingestion, transformation, and processing using AWS services and Apache Kafka.

---

## ⚙️ Architecture Overview
**Flow:**  
**Kafka → S3 (Raw Data) → Lambda Trigger → AWS Glue (PySpark ETL) → S3 (Processed Data)**

1. **Apache Kafka** – Streams incoming retail transactions in real time.  
2. **AWS S3** – Stores raw data from Kafka producers.  
3. **AWS Lambda** – Automatically triggers AWS Glue when new files arrive in S3.  
4. **AWS Glue (PySpark)** – Cleans, transforms, and aggregates data.  
5. **Processed Layer** – Stores curated output for analytics or visualization.

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
