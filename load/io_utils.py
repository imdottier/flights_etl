import logging
import os
from dotenv import load_dotenv
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from load.database import transaction_context

load_dotenv()

POSTGRE_DB_NAME = os.getenv("POSTGRE_DB_NAME")
POSTGRE_DB_USER = os.getenv("POSTGRE_DB_USER")
POSTGRE_DB_PASSWORD = os.getenv("POSTGRE_DB_PASSWORD")
POSTGRE_DB_HOST = os.getenv("POSTGRE_DB_HOST")


pg_url = f"jdbc:postgresql://{POSTGRE_DB_HOST}/{POSTGRE_DB_NAME}"
pg_properties = {
    "user": POSTGRE_DB_USER,
    "password": POSTGRE_DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}


def load_table_to_postgres(
    spark: SparkSession,
    df: DataFrame,
    target_schema: str,
    target_table: str,
    strategy: str,
    pk_cols: list[str] | None = None,
    partition_key_col: str | None = None, 
    confirm_truncate: bool = False,
    cursor=None,
):
    # --- 1. Input Validation ---
    if strategy == "upsert" and not pk_cols:
        raise ValueError("pk_cols is required for 'upsert' strategy.")
    if strategy == "delete_insert" and not partition_key_col:
        raise ValueError("partition_key_col is required for 'delete_insert' strategy.")
    if strategy == "truncate_insert" and not confirm_truncate:
        raise ValueError(
            "The 'truncate_insert' strategy is destructive. "
            "You must explicitly pass `confirm_truncate=True` to proceed."
        ) 

    full_table_name_str = f"{target_schema}.{target_table}"
    temp_table_name = f"temp_{target_schema}_{target_table}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    try:
        # --- 2. Write to Temporary Table ---
        logging.info(f"Writing to temp table: {temp_table_name}...")
        df.write.jdbc(url=pg_url, table=temp_table_name, mode="overwrite", properties=pg_properties)
        logging.info(f"Successfully wrote to temporary table.")

        all_columns = ", ".join(f'"{c}"' for c in df.columns)

        with transaction_context(cursor) as cur:
            # --- 3. Merge into Target Table ---
            if strategy == "upsert":
                pk_columns_str = ", ".join(f'"{c}"' for c in pk_cols)
                update_set_str = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col not in pk_cols])

                if not update_set_str:
                    merge_sql = f"""
                        INSERT INTO {full_table_name_str} ({all_columns})
                        SELECT {all_columns} FROM {temp_table_name}
                        ON CONFLICT ({pk_columns_str}) DO NOTHING
                    """
                else:
                    merge_sql = f"""
                        INSERT INTO {full_table_name_str} ({all_columns})
                        SELECT {all_columns} FROM {temp_table_name}
                        ON CONFLICT ({pk_columns_str}) DO UPDATE SET
                            {update_set_str};
                    """

                logging.info(f"Executing merge SQL from {temp_table_name} to {full_table_name_str}...")
                cur.execute(merge_sql)
                logging.info("Merge operation complete.")

            elif strategy == "delete_insert":
                partition_values = tuple([row[0] for row in df.select(partition_key_col).distinct().collect()])

                delete_sql = f"DELETE FROM {full_table_name_str} WHERE {partition_key_col} IN %s;"
                insert_sql = f"INSERT INTO {full_table_name_str} ({all_columns}) SELECT {all_columns} FROM {temp_table_name};"

                logging.info(f"Executing delete for partitions in {full_table_name_str}...")
                # Pass the values as the second argument to execute()
                cur.execute(delete_sql, (partition_values,))
                logging.info(f"Deleted {cur.rowcount} rows. Now inserting new data...")
                cur.execute(insert_sql)
                logging.info(f"Inserted {cur.rowcount} rows. Transaction complete.")

            elif strategy == "truncate_insert":
                logging.warning("Truncate_insert is dangerous. Use with caution.")

                sql_transaction = f"""
                    TRUNCATE TABLE {full_table_name_str};
                    INSERT INTO {full_table_name_str} SELECT * FROM {temp_table_name};
                    SELECT {all_columns} FROM {temp_table_name};
                """

                logging.info(f"Executing SQL transaction from {temp_table_name} to {full_table_name_str}...")
                cur.execute(sql_transaction)
                logging.info("Transaction complete.")

    finally:
        logging.info(f"Dropping temporary table {temp_table_name}...")
        try:
            with transaction_context(cursor) as cur:
                cur.execute(f"DROP TABLE IF EXISTS {temp_table_name};")
            logging.info("Temporary table dropped successfully.")
        except Exception as e:
            logging.error(f"Failed to drop temporary table {temp_table_name}: {e}", exc_info=True)


def read_df_from_postgres(
    spark: SparkSession,
    table_schema: str,
    table_name: str
) -> DataFrame:
    full_table_name = f"{table_schema}.{table_name}"
    logging.info(f"Reading {full_table_name} from Postgres...")

    try:
        df = spark.read.jdbc(url=pg_url, table=full_table_name, properties=pg_properties)
        logging.info(f"Successfully read {df.count()} rows from {table_name}")
        return df
    
    except Exception as e:
        logging.error(f"Error reading {full_table_name}: {e}", exc_info=True)
        raise
