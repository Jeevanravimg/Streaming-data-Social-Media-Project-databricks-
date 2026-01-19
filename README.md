📘 Real-Time Social Media Streaming Project (Databricks)
📌 Project Overview

This project implements a real-time social media streaming data pipeline using Databricks, Delta Live Tables (DLT), and Unity Catalog.
Synthetic social media data (users, posts, comments, likes) is continuously generated and ingested as JSON files, processed through a Bronze → Silver → Gold Medallion Architecture, and exposed for analytics and dashboards.

The project demonstrates production-grade streaming ingestion, data quality enforcement, transformations, and business-level aggregations.

🏗️ Architecture Overview
Fake Data Generator (JSON)
        ↓
Unity Catalog Volume (Raw JSON Files)
        ↓
Bronze Layer (DLT Streaming Tables)
        ↓
Silver Layer (Cleaned & Validated Tables)
        ↓
Gold Layer (Aggregated Analytics Tables)
        ↓
Dashboards & Insights

🛠️ Technologies Used

Databricks

Delta Live Tables (DLT)

Apache Spark Structured Streaming

Unity Catalog

Python

Faker

GitHub

📂 Project Structure
Streaming-data-Social-Media-Project-databricks/
│
├── POC_Streaming_Jeevan/
│   └── Data_Generation_JSON        # Continuous JSON data generator
│
├── Social_Media_Pipeline/
│   └── transformations/
│       ├── Bronze_Layer.py         # DLT Bronze streaming ingestion
│       ├── Silver_Layer.py         # DLT Silver cleaning & quality checks
│       └── Gold_Layer.py           # DLT Gold aggregations
│
├── Social_Media_Dashboards         # Databricks dashboards
├── README.md

📦 Data Generation (Streaming Source)
🔹 Description

Synthetic social media data is generated continuously using the Faker library.
The generator runs multiple threads in parallel to simulate real-time event streams.

🔹 Data Types Generated

Users

Posts

Comments

Likes / Reactions

🔹 Storage Location (Unity Catalog Volume)
/Volumes/kusha_solutions/jeevan_streaming/my_volume/Raw_JSON_Files/
├── users/
├── posts/
├── comments/
└── likes/

🔹 Key Characteristics

JSON format

Continuous file generation

Includes randomness and high-volume interaction patterns

Fully synthetic (no real user data)

🥉 Bronze Layer — Raw Streaming Ingestion
🔹 Description

The Bronze layer ingests raw JSON files using Databricks Auto Loader with Spark Structured Streaming.

🔹 Features

Streaming ingestion (readStream)

Explicit schema enforcement

Metadata capture:

Ingestion timestamp

Source file path

Fault-tolerant schema tracking

🔹 Bronze Tables

bronze_users

bronze_posts

bronze_comments

bronze_likes

🔹 Schema Checkpoint Location
/Volumes/kusha_solutions/jeevan_streaming/my_volume/autoloader_schema/

🥈 Silver Layer — Data Cleaning & Quality Enforcement
🔹 Description

The Silver layer cleans and standardizes data from the Bronze layer and applies data quality rules using DLT expectations.

🔹 Key Transformations

Deduplication based on primary keys

Null handling with default values

Text standardization (trim, lowercase)

Processed timestamp for auditing

🔹 Data Quality Rules (DLT Expectations)

Mandatory fields must not be null

Email format validation

Referential integrity checks

🔹 Silver Tables

silver_users

silver_posts

silver_comments

silver_likes

🥇 Gold Layer — Business Aggregations
🔹 Description

The Gold layer produces analytics-ready tables optimized for reporting and dashboards.

🔹 Gold Tables Created
1️⃣ Top Active Users

gold_top_active_users

Top 10 users by total engagement

Engagement score = likes + comments

2️⃣ Popular Posts

gold_popular_posts

Top 10 posts by combined likes and comments

3️⃣ Daily Engagement Trends

gold_daily_engagement_trends

Daily likes, comments, and total engagement trends

4️⃣ User Engagement Summary

gold_user_engagement_summary

Average likes and comments per post per user

📊 Dashboards
🔹 Description

Databricks dashboards are built on top of Gold tables to visualize real-time insights.

🔹 Dashboard Queries
SELECT * FROM kusha_solutions.jeevan_streaming.gold_top_active_users;
SELECT * FROM kusha_solutions.jeevan_streaming.gold_popular_posts;
SELECT * FROM kusha_solutions.jeevan_streaming.gold_daily_engagement_trends;
SELECT * FROM kusha_solutions.jeevan_streaming.gold_user_engagement_summary;

▶️ How to Run the Project (Databricks)
Step 1: Clone Repository
git clone https://github.com/Jeevanravimg/real-time-social-media-streaming.git

Step 2: Open in Databricks

Workspace → Home → Create → Git Folder

Paste repository URL

Step 3: Run Data Generator

Open POC_Streaming_Jeevan/Data_Generation_JSON

Run notebook to start continuous JSON generation

Step 4: Configure DLT Pipeline

Create a Delta Live Tables pipeline

Add:

Bronze_Layer.py

Silver_Layer.py

Gold_Layer.py

Set target schema:

kusha_solutions.jeevan_streaming

Step 5: Start Pipeline

Run pipeline in Continuous or Triggered mode

🔐 Data Disclaimer

All data used in this project is synthetically generated using Faker and does not represent real users or social media activity.

🎯 Key Learnings

Real-time streaming ingestion with Auto Loader

Delta Live Tables with expectations

Medallion architecture for streaming workloads

Production-ready data quality enforcement

Analytical aggregation for dashboards

👤 Author

Jeevan M G
Databricks | Data Engineering | Lakehouse Architecture