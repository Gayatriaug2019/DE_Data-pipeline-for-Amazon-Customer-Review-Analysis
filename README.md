# 🛒 Data Pipeline for Amazon Customer Review Analysis

## 📌 Project Overview
This project implements an **end-to-end data pipeline** to analyze **Amazon customer reviews** stored in **Parquet format** on AWS S3.  
The pipeline is designed to **automatically extract, clean, transform, and load** large-scale review data into a **remote SQL database**, followed by **analytical querying and reporting**.

The entire pipeline is executed on **Google Colab**, leveraging **PySpark for big data processing**, **SQLite for relational storage**, and **Power BI for reporting and visualization**.  
The process is **fully automated with no manual intervention**.

---

## 🧰 Technologies Used
- **Programming Language:** Python  
- **Big Data Processing:** PySpark  
- **Cloud Storage:** AWS S3  
- **Database:** SQLite  
- **Query Language:** SQL  
- **Visualization:** Power BI  
- **Execution Platform:** Google Colab  

---

## 📊 Dataset Details
- **Source:** AWS S3  
- **Format:** Parquet  
- **File Name:** `awsdata.parquet`  
- **Download Method:** URL-based automated download  
- **Content Includes:**
  - Product details
  - Customer information
  - Review ratings
  - Helpful votes
  - Review text
  - Review timestamps

---

## 📂 Project Structure
amazon-review-pipeline/
│
├── data/
│ └── awsdata.parquet
│ └── amazon_reviews.db
│ └── amazon_analysis_reviews.py
│ └── analysis_queries.txt
│
└── README.md

---

## 🎯 Problem Statement
The objective of this project is to:
- Analyze Amazon customer reviews at scale
- Clean and standardize review data
- Load transformed data into a SQL database
- Perform analytical queries to derive business insights
- Enable reporting and visualization for stakeholders

---

## 🛠️ Pipeline Workflow

### 🔹 Task 1: Data Extraction
- Download Parquet data from AWS S3 using URL
- Load data into a **PySpark DataFrame**
- Validate schema and record counts

**Outcome:**  
Dataset available in PySpark for processing.

---

### 🔹 Task 2: Data Cleaning & Preprocessing
- Remove duplicate records
- Handle missing/null values
- Correct data types:
  - `review_date` → Date
  - `star_rating` → Integer
- Standardize categorical fields:
  - Product category
  - Marketplace

**Outcome:**  
Cleaned and consistent dataset ready for transformation.

---

### 🔹 Task 3: Data Transformation
- Convert review dates to `YYYY-MM-DD` format
- Normalize text fields (`review_body`, `review_headline`)
- Create derived fields:
  - `review_month`
  - `review_year`

**Outcome:**  
Optimized dataset for analytical querying.

---

### 🔹 Task 4: Load Data into SQL Database
- Database used: **SQLite**
- Database file:
amazon_reviews.db
- Create structured schema with indexing
- Load transformed data using batch inserts

**Outcome:**  
Data successfully persisted in relational format.

---

### 🔹 Task 5: Analytical Queries
The following SQL-based analyses are performed:

1. **Top 10 products with the most reviews**
2. **Average review rating per month for each product**
3. **Total number of votes per product category**
4. **Products with highest occurrences of the word “awful”**
5. **Products with highest occurrences of the word “awesome”**
6. **Most controversial reviews (high votes, low helpful percentage)**
7. **Most commonly reviewed product per year**
8. **Users who wrote the most reviews**

**Outcome:**  
Actionable insights into customer sentiment and product performance.

---

### 🔹 Task 6: Monitoring & Optimization (Optional)
- Indexing applied on:
- `product_title`
- `customer_id`
- `review_date`
- Query optimization through:
- Date-based filtering
- Aggregation strategies

**Outcome:**  
Improved query performance on large datasets.

---

### 🔹 Task 8: Reporting & Visualization (Power BI)
- SQLite database connected to Power BI
- Dashboards created to visualize:
- Average ratings
- Most reviewed products
- Sentiment trends
- Customer activity

**Outcome:**  
Interactive dashboards providing real-time insights.

---

## ▶️ How to Run the Project (Google Colab)

1. Open **Google Colab**
2. Upload `amazon_review_pipeline.ipynb`
3. Run all cells sequentially
4. Parquet data is automatically downloaded
5. SQLite database is created:
amazon_reviews.db

6. Use Power BI to connect and visualize data

---

## 📈 Outputs
- **Database:** `amazon_reviews.db`
- **Processed Dataset:** Cleaned & transformed review data
- **SQL Insights:** Analytical query results
- **Dashboard:** Power BI interactive reports


## 👤 Author
**Gayatri**  
Python Backend Engineer | Data Engineer  
