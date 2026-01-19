# Real-Time Social Media Streaming Project (Databricks)

## Project Overview
This project implements a **real-time social media streaming data pipeline** using **Databricks**, **Delta Live Tables (DLT)**, and **Unity Catalog**.  
Synthetic social media data (users, posts, comments, likes) is continuously generated as JSON files and processed using a **Bronze → Silver → Gold Medallion Architecture** for analytics and dashboards.

---

## Architecture Overview

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


---

## Technologies Used
- Databricks
- Delta Live Tables (DLT)
- Apache Spark Structured Streaming
- Unity Catalog
- Python
- Faker
- GitHub

---

## Project Structure

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

---

## Data Generation (Streaming Source)

Synthetic social media data is continuously generated using the **Faker** library.
Parallel threads simulate real-time event streams for:

- Users
- Posts
- Comments
- Likes / Reactions

### Storage Location (Unity Catalog Volume)

/Volumes/kusha_solutions/jeevan_streaming/my_volume/Raw_JSON_Files/

- users/
- posts/
- comments/
- likes/

All data is **synthetic and safe for public sharing**.

---

## Bronze Layer — Raw Streaming Ingestion

The Bronze layer ingests raw JSON files using **Databricks Auto Loader** and Spark Structured Streaming.

### Features
- Streaming ingestion (`readStream`)
- Schema enforcement
- Ingestion timestamp
- Source file metadata

### Bronze Tables
- bronze_users
- bronze_posts
- bronze_comments
- bronze_likes

### Schema Checkpoint Location
/Volumes/kusha_solutions/jeevan_streaming/my_volume/autoloader_schema/

---

## Silver Layer — Data Cleaning & Quality Enforcement

The Silver layer cleans and standardizes data from the Bronze layer using **DLT expectations**.

### Transformations
- Deduplication
- Null handling
- Text standardization
- Processed timestamp

### Data Quality Rules
- Mandatory fields must not be null
- Email format validation
- Key integrity checks

### Silver Tables
- silver_users
- silver_posts
- silver_comments
- silver_likes

---

## Gold Layer — Business Aggregations

The Gold layer produces analytics-ready tables.

### Gold Tables

- gold_top_active_users  
  Top 10 users based on total likes and comments

- gold_popular_posts  
  Top 10 posts by combined engagement

- gold_daily_engagement_trends  
  Daily likes, comments, and total engagement

- gold_user_engagement_summary  
  Average likes and comments per post per user

---

## Dashboards

Dashboards are built on top of Gold tables 

---

## How to Run the Project (Databricks)

1. Clone the repository:
   git clone https://github.com/Jeevanravimg/real-time-social-media-streaming.git

2. In Databricks:
   Workspace → Home → Create → Git Folder  
   Paste the repository URL

3. Run the data generator:
   Open `POC_Streaming_Jeevan/Data_Generation_JSON`  
   Execute the notebook to start streaming JSON generation

4. Create a Delta Live Tables pipeline:
   Add:
   - Bronze_Layer.py
   - Silver_Layer.py
   - Gold_Layer.py

5. Set target schema:
   kusha_solutions.jeevan_streaming

6. Start the pipeline in **Continuous** or **Triggered** mode

---

## Data Disclaimer

All datasets used in this project are **synthetically generated using Faker** and do not represent real users or real social media activity.

---

## Key Learnings
- Real-time streaming ingestion with Auto Loader
- Delta Live Tables with data quality expectations
- Medallion architecture for streaming workloads
- Production-grade aggregations and dashboards

---

## Author
**Jeevan M G**  
Databricks | Data Engineering | Lakehouse Architecture
