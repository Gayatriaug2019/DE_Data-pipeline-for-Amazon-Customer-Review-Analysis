!pip install boto3

from io import BytesIO
import pandas as pd
import boto3
import sqlite3
import requests
import os

# ---------------- AWS CONFIG ---------------- 

aws_access_key = ""
aws_secret_key = ""

# ---------------- CLIENT ---------------- 

s3 = boto3.client( "s3", 
                  aws_access_key_id=aws_access_key, 
                  aws_secret_access_key=aws_secret_key 
                   )

#--------- DOWNLOAD FILE USING URL---------

def download_file(url, local_path): 
    """ Download a file from a given URL and save it locally in Colab. 
    Parameters: url (str): The direct link to the file. 
    local_path (str): Local path where the file will be saved. 
    Returns: str: Path to the saved file. """ 

    try: 
      response = requests.get(url, stream=True) 
      response.raise_for_status() # raise error for bad status codes 
    
      # Ensure directory exists 
      os.makedirs(os.path.dirname(local_path), exist_ok=True) 
      # Write file in chunks 
      with open(local_path, "wb") as f: 
        for chunk in response.iter_content(chunk_size=8192): 
          if chunk: f.write(chunk) 
          
      print(f"File downloaded successfully and saved to {local_path}") 
      return local_path 
    except Exception as e: 
      print(f"Download failed: {e}") 
      return None

# ---------------- UPLOAD FUNCTION ----------------

def upload_to_s3(file_path, bucket, object_name=None):
    """Upload a file to S3 bucket."""
    if object_name is None:
        object_name = os.path.basename(file_path)
    try:
        s3.upload_file(file_path, bucket, object_name)
        print(f"Uploaded {file_path} to s3://{bucket}/{object_name}")
    except Exception as e:
        print(f"Upload failed: {e}")

# ---------------- STEP 1: DOWNLOAD FROM S3 ----------------
def download_from_s3(bucket, object_name, file_path):
    s3.download_file(bucket, object_name, file_path)

# ----------------STEP 2: EXTRACT DATA ---------------------
def read_parquet_file(file_path):
    """
    Read a parquet file from Colab and return as a Pandas DataFrame.
    Parameters:
        file_path (str): Path to the parquet file.
    Returns:
        pd.DataFrame: DataFrame containing the parquet data.
    """
    try:
        df = pd.read_csv(file_path)
        print("Parquet file loaded successfully")
        return df
    except Exception as e:
        print(f"Failed to read parquet file: {e}")
        return None

# ------------------------------
# Step 3: Data Cleaning & Preprocessing
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
# Step 4: Data Transformation
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
# Step 5: Load Data into SQLite
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


# -------------- Main ------------------

def run_etl():
    # AWS S3 Configuration
    url = input("Enter the url ")
    bucket_name = "etl-worflow-3"
    file_key = "awsdata.parquet"
    local_file_name = "downloaded_file.parquet"
    local_path="/content/downloaded_file.parquet"
    extract_path = "/content/"

    # Download file via link
    download_file(url, local_path="/content/downloaded_file.parquet")

    # upload to s3
    upload_to_s3(local_path, bucket_name, object_name=None)

    # STEP 1: Download from s3
    download_from_s3(bucket_name,local_file_name,os.path.join(file_key))

    # STEP 2: Data extraction
    df = read_parquet_file(file_path="/content/awsdata.parquet")

    # Step 3: Clean data
    df_cleaned = clean_data(df)

    # Step 4: Transform data
    df_transformed = transform_data(df_cleaned)

    # Step 5: Load data into SQLite
    load_data_to_sqlite(df_transformed)

# Run the ETL pipeline
if __name__ == "__main__":
    run_etl()
