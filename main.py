from etl.extract import extract_jobs
from etl.transform import transform_bronze_to_silver
from etl.load import load_csv_to_postgres
import json

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
load_csv_to_postgres(
    csv_path="data/silver_jobs.csv",
    table_name="jobs_silver",
    pg_username="postgres",
    pg_password="mavi123",   
    pg_host="localhost",
    pg_port="5432",
    pg_db="jobs_db"
)

print("Data loaded to Localhost PostgreSQL.")


# =============================
#  Load to Docker PostgreSQL 
# =============================
load_csv_to_postgres(
    csv_path="data/silver_jobs.csv",
    table_name="jobs_silver",
    pg_username="postgres",
    pg_password="mavi123",  
    pg_host="localhost",
    pg_port="5432",
    pg_db="jobs_db"
)

print("Data loaded to Docker PostgreSQL.")