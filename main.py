from etl.extract import extract_jobs
from etl.transform import transform_bronze_to_silver
from etl.load import load_csv_to_postgres
import json
from dotenv import load_dotenv
import os

BASE_URL = "https://www.arbeitnow.com/api/job-board-api"

# ========================
#  Extract (Bronze)
# ========================
jobs = extract_jobs(BASE_URL)

with open("data/bronze_jobs.json", "w", encoding="utf-8") as f:
    json.dump({"data": jobs}, f, indent=4)

print("Bronze layer saved.")

# ========================
#  Transform (Silver)
# ========================
transform_bronze_to_silver(
    "data/bronze_jobs.json",
    "data/silver_jobs.csv"
)

print("Silver layer created.")

# =============================
#  Load to PostgreSQL Locally
# =============================
load_dotenv()
load_csv_to_postgres(
    csv_path="data/silver_jobs.csv",
    table_name="jobs_silver",
    pg_username=os.getenv("DB_USERNAME"),
    pg_password=os.getenv("DB_PASSWORD"),   
    pg_host=os.getenv("DB_HOST"),
    pg_port=os.getenv("DB_PORT"),
    pg_db=os.getenv("DB_NAME")
)

print("Data loaded to Localhost PostgreSQL.")


# =============================
#  Load to Docker PostgreSQL 
# =============================
load_csv_to_postgres(
    csv_path="data/silver_jobs.csv",
    table_name="jobs_silver",
    pg_username=os.getenv("DB_USERNAME"),
    pg_password=os.getenv("DB_PASSWORD"),  
    pg_host=os.getenv("DB_HOST"),
    pg_port=os.getenv("DB_PORT"),
    pg_db=os.getenv("DB_NAME")
)

print("Data loaded to Docker PostgreSQL.")