# Commerce Analytics Platform on AWS  
**Serverless Data Lake | Glue ETL | Athena | EventBridge | Lambda**

---

## 📌 Overview
This project implements an **end-to-end, serverless commerce analytics platform** on AWS using a modern **data lake architecture (Bronze–Silver–Gold)**.

It ingests raw transactional data, performs scalable transformations, and enables fast, SQL-based analytics for business reporting — **without managing servers**.

The platform is designed to reflect **real-world enterprise data engineering workflows**, emphasizing automation, reliability, and cost efficiency.

---

## 🏗️ Architecture

### Core Services Used
- **Amazon S3** – Centralized data lake (Bronze, Silver, Gold layers)
- **AWS Glue** – ETL jobs for data cleansing, transformation, and modeling
- **AWS Lambda** – Pipeline orchestration and Glue job triggering
- **Amazon EventBridge** – Scheduled automation
- **AWS Glue Data Catalog** – Metadata management
- **Amazon Athena** – SQL analytics on curated datasets

---

## 🔁 Data Flow
1. Raw commerce data lands in **S3 Bronze**
2. Glue ETL cleans and standardizes data into **S3 Silver**
3. Dimensional models (fact & dimension tables) are created in **S3 Gold**
4. Athena queries enable KPI and trend analysis
5. EventBridge + Lambda automate recurring executions

---

## 🧱 Data Lake Structure
```text
S3 Data Lake
├── bronze/    # Raw, immutable source data
├── silver/    # Cleaned and standardized data
└── gold/      # Analytics-ready fact & dimension tables
