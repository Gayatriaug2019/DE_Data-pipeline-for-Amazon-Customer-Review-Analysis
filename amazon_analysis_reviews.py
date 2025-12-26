import boto3
import pandas as pd
import sqlite3
from io import BytesIO

# -------------------------
# Step 1: Data Extraction
# -------------------------
def extract_data_from_s3(bucket_name, file_key, aws_access_key, aws_secret_key):
    print(" Extracting data from S3...")

    # Connect to S3
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    # Download file from S3
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    data = obj["Body"].read()

    # Read Parquet data into DataFrame
    df = pd.read_parquet(BytesIO(data))
    print(f"Successfully extracted {len(df)} rows from {file_key}")
    
    return df

# ------------------------------
# Step 2: Data Cleaning & Preprocessing
# ------------------------------
def clean_data(df):
    print("Cleaning data...")

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Handle missing values
    df.fillna({"review_headline": "No headline", "review_body": "No review"}, inplace=True)

    # Correct data types
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce").dt.date
    df["star_rating"] = pd.to_numeric(df["star_rating"], errors="coerce").fillna(0).astype(int)

    # Standardize text fields
    df["product_category"] = df["product_category"].str.strip().str.lower()

    print(f"Cleaned data with {len(df)} rows remaining.")
    return df

# -------------------------
# Step 3: Data Transformation
# -------------------------
def transform_data(df):
    print(" Transforming data...")

    # Add new features
    df["review_month"] = pd.to_datetime(df["review_date"]).dt.strftime("%Y-%m")

    # Normalize review text
    df["review_body"] = df["review_body"].str.lower().str.replace("[^a-z0-9\s]", "", regex=True)

    print("Data transformation complete.")
    return df

# -------------------------
# Step 4: Load Data into SQLite
# -------------------------
def load_data_to_sqlite(df, db_name="amazon_reviews.db"):
    print("Creating database and loading data...")

    # Create SQLite connection
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Correct CREATE TABLE statement with actual schema
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS amazon_reviews (
        review_date DATE,
        marketplace TEXT,
        customer_id INTEGER,
        review_id TEXT PRIMARY KEY,
        product_id TEXT,
        product_parent REAL,
        product_title TEXT,
        product_category TEXT,
        star_rating INTEGER,
        helpful_votes INTEGER,
        total_votes INTEGER,
        vine BOOLEAN,
        verified_purchase BOOLEAN,
        review_headline TEXT,
        review_body TEXT
    );
    """
    
    # Execute CREATE TABLE statement
    cursor.execute(create_table_sql)

    # Insert data into the table
    df.to_sql("amazon_reviews", conn, if_exists="replace", index=False)

    # Create recommended indexes for better query performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_product_title ON amazon_reviews(product_title)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_customer_id ON amazon_reviews(customer_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_review_date ON amazon_reviews(review_date)")

    conn.commit()
    conn.close()

    print("Data successfully loaded into SQLite database!")

def run_etl():
    # AWS S3 Configuration
    bucket_name = "miniproject4"
    file_key = "awsdata.parquet"
    aws_access_key = ""
    aws_secret_key = ""

    # Step 1: Extract data
    df = extract_data_from_s3(bucket_name, file_key, aws_access_key, aws_secret_key)

    # Step 2: Clean data
    df_cleaned = clean_data(df)

    # Step 3: Transform data
    df_transformed = transform_data(df_cleaned)

    # Step 4: Load data into SQLite
    load_data_to_sqlite(df_transformed)

# Run the ETL pipeline
if __name__ == "__main__":
    run_etl()
