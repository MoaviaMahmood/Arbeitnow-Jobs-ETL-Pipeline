import pandas as pd
from sqlalchemy import create_engine, text


def load_csv_to_postgres(
    csv_path,
    table_name,
    pg_username,
    pg_password,
    pg_host='localhost',
    pg_port='5433',
    pg_db='jobs_db'
):
    """
    Load CSV file into PostgreSQL table and return row count.
    """

    # Create engine
    engine = create_engine(
        f'postgresql://{pg_username}:{pg_password}@{pg_host}:{pg_port}/{pg_db}'
    )

    # Read CSV
    df = pd.read_csv(csv_path)

    # Load into PostgreSQL
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    # Verify row count
    query = f"SELECT COUNT(*) FROM {table_name};"

    with engine.connect() as connection:
        result = connection.execute(text(query))
        count = result.fetchone()[0]

    print(f"Loaded {count} records into '{table_name}' table.")

    return count
