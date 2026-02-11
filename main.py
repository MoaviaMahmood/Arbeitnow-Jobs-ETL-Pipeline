from etl.extract import extract_jobs
from etl.transform import transform_bronze_to_silver
from etl.load import create_table, load_csv_to_postgres
import json

BASE_URL = "https://www.arbeitnow.com/api/job-board-api"

# Extract (Bronze)
jobs = extract_jobs(BASE_URL)

# Save Bronze JSON
with open("data/bronze_jobs.json", "w", encoding="utf-8") as f:
    json.dump({"data": jobs}, f, indent=4)

print("Bronze layer saved.")

# Transform (Silver)
transform_bronze_to_silver(
    "data/bronze_jobs.json",
    "data/silver_jobs.csv"
)

# Load to Postgres using docker
create_table()

load_csv_to_postgres("data/silver_jobs.csv")
