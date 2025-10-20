import psycopg2
import os
from dotenv import load_dotenv
import logging
from contextlib import contextmanager

load_dotenv()

POSTGRE_DB_NAME = os.getenv("POSTGRE_DB_NAME")
POSTGRE_DB_USER = os.getenv("POSTGRE_DB_USER")
POSTGRE_DB_PASSWORD = os.getenv("POSTGRE_DB_PASSWORD")
POSTGRE_DB_HOST = os.getenv("POSTGRE_DB_HOST")


@contextmanager
def get_db():
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=POSTGRE_DB_NAME,
            user=POSTGRE_DB_USER,
            password=POSTGRE_DB_PASSWORD,
            host=POSTGRE_DB_HOST
        )
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error in database: {e}", exc_info=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@contextmanager
def transaction_context(existing_cursor=None):
    """
    A context manager that either uses an existing database cursor
    or creates a new connection and transaction context.
    """
    if existing_cursor:
        yield existing_cursor
    else:
        with get_db() as new_cursor:
            yield new_cursor

def connect():
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=POSTGRE_DB_NAME,
            user=POSTGRE_DB_USER,
            password=POSTGRE_DB_PASSWORD,
            host=POSTGRE_DB_HOST
        )

        cur = conn.cursor()

        print("POSTGRESQL database version:")
        cur.execute("SELECT version()")

        db_version = cur.fetchone()
        print(db_version)

        cur.close()
    
    except (Exception, psycopg2.DatabaseError) as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    connect()