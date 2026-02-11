import psycopg
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration from environment
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "jobs_db"),
    "user": os.getenv("DB_USER", "jobs_user"),
    "password": os.getenv("DB_PASSWORD", "jobs_password"),
    "connect_timeout": int(os.getenv("DB_TIMEOUT", 10)),
    "options": "-c search_path=public"  # PostgreSQL 17 compatible
}

def create_table():
    """Create jobs_silver table if it doesn't exist."""
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Enable UUID extension (PostgreSQL 17 feature)
                cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
                
                # Create table with PostgreSQL 17 syntax
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS jobs_silver (
                        slug_title TEXT,
                        job_id TEXT PRIMARY KEY,
                        company_name TEXT,
                        title TEXT NOT NULL,
                        description TEXT,
                        description_length INT GENERATED ALWAYS AS 
                            (LENGTH(COALESCE(description, ''))) STORED,  -- PostgreSQL 17 feature
                        remote INT CHECK (remote IN (0, 1)),
                        url TEXT,
                        city TEXT,
                        region TEXT,
                        country TEXT,
                        created_at TIMESTAMP,
                        created_date DATE,
                        created_year INT,
                        created_month INT,
                        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        -- PostgreSQL 17 MERGE support
                        etl_version TEXT DEFAULT '1.0.0'
                    );
                    
                    -- Create indexes if they don't exist
                    CREATE INDEX IF NOT EXISTS idx_created_date ON jobs_silver(created_date);
                    CREATE INDEX IF NOT EXISTS idx_company_name ON jobs_silver(company_name);
                    CREATE INDEX IF NOT EXISTS idx_remote ON jobs_silver(remote);
                    
                    -- Partial index for active jobs (PostgreSQL 17 feature)
                    CREATE INDEX IF NOT EXISTS idx_recent_jobs ON jobs_silver(created_date) 
                        WHERE created_date > CURRENT_DATE - INTERVAL '30 days';
                """)
            conn.commit()
        logger.info("Table created/verified successfully with PostgreSQL 17 features.")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        raise

def load_csv_to_postgres(csv_path):
    """Load CSV data into Postgres using PostgreSQL 17 features."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Use PostgreSQL 17 MERGE (more efficient than INSERT...ON CONFLICT)
                with open(csv_path, "r", encoding="utf-8") as f:
                    next(f)  # Skip header
                    
                    # Create temporary table for staging
                    cur.execute("""
                        CREATE TEMP TABLE jobs_staging (LIKE jobs_silver INCLUDING ALL);
                    """)
                    
                    # Copy data to staging
                    cur.copy("""
                        COPY jobs_staging (
                            slug_title,
                            job_id,
                            company_name,
                            title,
                            description,
                            remote,
                            url,
                            city,
                            region,
                            country,
                            created_at,
                            created_date,
                            created_year,
                            created_month
                        )
                        FROM STDIN WITH (FORMAT CSV, NULL 'unknown', HEADER false)
                    """, f)
                
                # MERGE statement (PostgreSQL 17+)
                cur.execute("""
                    MERGE INTO jobs_silver AS target
                    USING jobs_staging AS source
                    ON target.job_id = source.job_id
                    WHEN MATCHED THEN
                        UPDATE SET 
                            title = source.title,
                            description = source.description,
                            remote = source.remote,
                            loaded_at = CURRENT_TIMESTAMP
                    WHEN NOT MATCHED THEN
                        INSERT (
                            slug_title, job_id, company_name, title, description,
                            remote, url, city, region, country, created_at,
                            created_date, created_year, created_month
                        ) VALUES (
                            source.slug_title, source.job_id, source.company_name, 
                            source.title, source.description, source.remote, source.url,
                            source.city, source.region, source.country, source.created_at,
                            source.created_date, source.created_year, source.created_month
                        );
                """)
                
                # Get row count
                cur.execute("SELECT COUNT(*) FROM jobs_staging")
                row_count = cur.fetchone()[0]
                
            conn.commit()
        
        logger.info(f"Successfully loaded {row_count} records into PostgreSQL 17.")
        return row_count
        
    except Exception as e:
        logger.error(f"Failed to load data to PostgreSQL: {e}")
        raise