import psycopg2
import os
from dotenv import load_dotenv
import logging
from utils.logging_utils import setup_logging

load_dotenv()

POSTGRES_DB_USER = os.getenv("POSTGRES_DB_USER")
POSTGRES_DB_PASSWORD = os.getenv("POSTGRES_DB_PASSWORD")
POSTGRES_DB_HOST = os.getenv("POSTGRES_DB_HOST")

POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME", "flights_dw")
SPARK_METASTORE_DB_NAME = os.getenv("SPARK_METASTORE_DB_NAME", "spark_metastore_prod")
AIRFLOW_DB_NAME = os.getenv("AIRFLOW_DB_NAME", "airflow")

DBS = [
    POSTGRES_DB_NAME,
    SPARK_METASTORE_DB_NAME,
    AIRFLOW_DB_NAME,
]

def create_db_if_not_exists(db_name):
    logging.info(f"Creating database {db_name}")
    conn = psycopg2.connect(
        dbname="postgres",
        user=POSTGRES_DB_USER,
        password=POSTGRES_DB_PASSWORD,
        host=POSTGRES_DB_HOST,
        port=5432,
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db_name,))
    if not cur.fetchone():
        logging.info(f"Database {db_name} does not exist. Creating...")
        cur.execute(f"CREATE DATABASE {db_name} OWNER {POSTGRES_DB_USER}")
    else:
        logging.info(f"Database {db_name} already exists.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    logger = setup_logging(log_to_file=False)
    logging.info("Creating databases...")
    for db_name in DBS:
        create_db_if_not_exists(db_name)