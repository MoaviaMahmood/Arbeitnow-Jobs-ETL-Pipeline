import psycopg
import os

def test_connection():
    # Try different connection methods
    connection_attempts = [
        {
            "host": "localhost",
            "port": 5432,
            "dbname": "jobs_db",
            "user": "jobs_user",
            "password": "jobs_password"
        },
        {
            "host": "127.0.0.1",
            "port": 5432,
            "dbname": "jobs_db",
            "user": "jobs_user",
            "password": "jobs_password"
        },
        {
            "host": "localhost",
            "port": 5432,
            "dbname": "jobs_db",
            "user": "jobs_user",
            # Try without password (for trust auth)
        }
    ]
    
    for i, params in enumerate(connection_attempts, 1):
        try:
            print(f"Attempt {i}: Connecting with {params}")
            with psycopg.connect(**params) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version();")
                    version = cur.fetchone()
                    print(f"Success! PostgreSQL version: {version[0]}")
                    return True
        except Exception as e:
            print(f"Failed: {e}")
    
    return False

if __name__ == "__main__":
    test_connection()