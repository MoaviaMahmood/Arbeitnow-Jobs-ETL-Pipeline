# Arbeitnow Jobs ETL Pipeline (Bronze → Silver → PostgreSQL)

An end-to-end **ETL pipeline** that extracts job postings from the Arbeitnow Job Board API, cleans and transforms the data, and loads it into a PostgreSQL database using modern PostgreSQL features (MERGE, generated columns, partial indexes).

---

## Project Overview

This project implements a layered data architecture:

```
Arbeitnow Jobs API
    │
    │  (HTTP GET Request)
    │  - Pagination handling
    │  - Error handling & status checks
    │
    ▼
Bronze Layer – Raw Data (JSON)
    │
    │  File: data/bronze_jobs.json
    │  - Raw API response
    │  - No transformations
    │  - Preserves original schema
    │  - Stored for reproducibility & auditing
    │
    ▼
Silver Layer – Cleaned Data (CSV)
    │
    │  File: data/silver_jobs.csv
    │
    │  Transformations:
    │  - Slug parsing → extract job_id
    │  - Title normalization
    │  - Location splitting (city, region, country)
    │  - Remote field normalization (0/1)
    │  - HTML tag removal
    │  - Encoding fixes (UTF-8 / Latin1 / CP1252)
    │  - HTML entity decoding
    │  - Remove Arbeitnow footer
    │  - Timestamp parsing (UNIX → date)
    │  - Whitespace normalization
    │
    ▼
PostgreSQL – Analytics Ready Layer
    │
    │  Table: jobs_silver
    │
    │  Schema Features:
    │  - job_id (Primary Key)
    │  - description_length (Generated column)
    │  - created_date (DATE)
    │  - loaded_at (ETL timestamp)
    │
    │  Performance Optimization:
    │  - B-tree indexes
    │  - Partial index (recent jobs)
    │  - Indexed remote flag
    │
    ▼
Analytics & BI / SQL Queries
    - Remote job analysis
    - Hiring trend analysis
    - Company job volume
    - Time-based aggregations

```
## Dashboard Preview

### Jobs by Company
<img width="1482" height="834" alt="Image" src="https://github.com/user-attachments/assets/c027807c-320e-4654-8ee8-93f0b7e8290c" />

### Jobs by City
<img width="1485" height="833" alt="Image" src="https://github.com/user-attachments/assets/29fbca01-9b76-4d12-8f29-286d556184ce" />

### Seniority Distribution
<img width="1488" height="834" alt="Image" src="https://github.com/user-attachments/assets/38eda240-0d7c-48e8-9d71-57c59ddbb87d" />


## Insights

- 73% of jobs are on-site
- Hamburg and Munich have the highest concentration
- Senior roles dominate the dataset

### Layers

- **Bronze Layer**
  - Raw JSON data extracted from API
  - Stored as `data/bronze_jobs.json`

- **Silver Layer**
  - Cleaned, normalized, structured data
  - Stored as `data/silver_jobs.csv`

- **Database Layer**
  - PostgreSQL table: `jobs_silver`
  - Indexed and optimized for analytics
  - Uses `MERGE` for upserts

---

## Features

- API extraction with error handling  
- Encoding fixes (UTF-8 / Latin1 / CP1252 issues)  
- HTML cleaning & entity decoding  
- Data normalization  
- Slug parsing (extract job_id)  
- Location splitting (city, region, country)  
- Timestamp parsing  
- PostgreSQL MERGE support  
- Generated columns  
- Partial indexing for recent jobs  
- Dockerized PostgreSQL  
- Environment-based configuration  

---

## Tech Stack

- Python 3.11+
- PostgreSQL 16/17
- psycopg (v3)
- Docker & Docker Compose
- python-dotenv
- Regex & HTML parsing

---

## Project Structure
```
.
├── etl/
│ ├── extract.py
│ ├── transform.py
│ ├── load.py
│
├── data/
│ ├── bronze_jobs.json
│ └── silver_jobs.csv
│
├── docker-compose.yml
├── main.py
├── test_connection.py
├── init-db.sql
└── README.md
```

---

## Setup Instructions

### Clone the Repository

```bash
git clone "https://github.com/MoaviaMahmood/Jobs_etl"
cd jobs-etl
```
## Create .env File

### Create a .env file in the root directory:
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=jobs_db
DB_USER=jobs_user
DB_PASSWORD=jobs_password
```

## Start PostgreSQL with Docker
```bash
docker compose up -d
```

## Check if the container is running:
```bash
docker ps
```

## Install Python Dependencies
```bash
pip install -r requirements.txt
```

## Run the Pipeline
```bash
python main.py
```

Pipeline steps:

- Extract jobs from API
- Save Bronze JSON
- Transform to Silver CSV
- Create PostgreSQL table
- Load data using MERGE

## Data Cleaning Logic
## Slug:
- Extracts numeric job_id
- Converts slug to readable title

Example:

senior-data-engineer-12345
→ slug_title: "senior data engineer"
→ job_id: "12345"

### Description Cleaning

- Fix encoding issues
- Decode HTML entities
- Preserve bullet points
- Remove HTML tags
- Remove Arbeitnow footer
- Normalize whitespace

## Remote Field

Converts multiple formats into binary:
```bash
| Input  | Output |
|--------|--------|
| true   | 1      |
| false  | 0      |
| yes    | 1      |
| no     | 0      |

```

## Created Date

Converts UNIX timestamp into:
- created_at (timestamp)
- created_date (date)
- created_year
- created_month

## PostgreSQL Table Schema

Table: jobs_silver
```bash
| Column              | Type           | Description               |
|---------------------|----------------|---------------------------|
| job_id              | TEXT (PK)      | Extracted from slug       |
| title               | TEXT           | Cleaned job title         |
| description         | TEXT           | Cleaned job description   |
| description_length  | INT            | Generated column          |
| remote              | INT            | 0/1 flag                  |
| city                | TEXT           | Extracted location        |
| region              | TEXT           | Extracted location        |
| country             | TEXT           | Extracted location        |
| created_date        | DATE           | Parsed from timestamp     |
| loaded_at           | TIMESTAMP      | ETL load time             |

```

### Indexes

- idx_created_date
- idx_company_name
- idx_remote
- idx_recent_jobs (partial index for recent jobs)

### Upsert Logic (PostgreSQL MERGE)

- If job_id exists → UPDATE
- If job_id does not exist → INSERT

Efficient and production-ready.

## Test Database Connection
```bash
python test_connection.py
```

## Example Queries
Get Recent Remote Jobs
```bash
SELECT title, company_name, city
FROM jobs_silver
WHERE remote = 1
ORDER BY created_date DESC;
```
Top Hiring Companies
```bash
SELECT company_name, COUNT(*) as total_jobs
FROM jobs_silver
GROUP BY company_name
ORDER BY total_jobs DESC;
```

## Future Improvements

- Add Gold layer (aggregated analytics)
- Add Airflow orchestration
- Add incremental loading
- Add logging to file
- Add unit tests
- Add CI/CD pipeline
- Add data validation (Great Expectations)


