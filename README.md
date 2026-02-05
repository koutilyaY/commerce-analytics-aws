Commerce Analytics Platform on AWS

Serverless Data Lake | Glue ETL | Athena | EventBridge | Lambda

📌 Overview

This project implements an end-to-end, serverless commerce analytics platform on AWS using a modern data lake architecture (Bronze–Silver–Gold).
It ingests raw transactional data, performs scalable transformations, and enables fast, SQL-based analytics for business reporting — all without managing servers.

The platform is designed to reflect real-world enterprise data engineering workflows, emphasizing automation, reliability, and cost efficiency.

🏗️ Architecture

Core Services Used

Amazon S3 – Centralized data lake (Bronze, Silver, Gold layers)

AWS Glue – ETL jobs for data cleansing, transformation, and modeling

AWS Lambda – Orchestration and Glue job triggering

Amazon EventBridge – Scheduled automation

AWS Glue Data Catalog – Metadata management

Amazon Athena – SQL analytics on curated datasets

Data Flow

Raw commerce data lands in S3 Bronze

Glue ETL cleans and standardizes data into S3 Silver

Dimensional models (fact & dimensions) are created in S3 Gold

Athena queries enable KPI and trend analysis

EventBridge + Lambda automate recurring executions

S3 Data Lake
├── bronze/        # Raw, immutable source data
├── silver/        # Cleaned and standardized data
└── gold/          # Analytics-ready fact & dimension tables

⚙️ Automation & Orchestration

EventBridge Scheduler triggers the pipeline on a defined schedule

Lambda orchestrator validates state and starts Glue jobs

Concurrency handling prevents overlapping job executions

Fully automated, hands-free pipeline execution

📊 Analytics (Amazon Athena)

Example analytical use cases:

Monthly revenue trends

Top products by sales

Customer purchase behavior

Country-level revenue distribution

Athena queries run directly on S3 Gold using the Glue Data Catalog.


