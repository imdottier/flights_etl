from load.database import transaction_context
import os
from dotenv import load_dotenv
import logging

load_dotenv()

POSTGRES_DB_USER = os.getenv("POSTGRES_DB_USER")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME", "flights_dw")
SPARK_METASTORE_DB_NAME = os.getenv("SPARK_METASTORE_DB_NAME", "spark_metastore_prod")
AIRFLOW_DB_NAME = os.getenv("AIRFLOW_DB_NAME", "airflow")

DBS = [
    POSTGRES_DB_NAME,
    SPARK_METASTORE_DB_NAME,
    AIRFLOW_DB_NAME,
]

if __name__ == "__main__":
    for db_name in DBS:
        with transaction_context() as cur:
            # Check if DB exists
            cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db_name,))
            if not cur.fetchone():
                cur.execute(f"CREATE DATABASE {db_name} OWNER {POSTGRES_DB_USER}")
                logging.info(f"Created database {db_name}")
            else:
                logging.info(f"Database {db_name} already exists")
