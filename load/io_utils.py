import logging
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, date_format, date_trunc, coalesce

from psycopg2 import sql
from psycopg2.extras import execute_values
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


# def load_table_with_partition(
#     spark: SparkSession,
#     df: DataFrame,
#     target_schema: str,
#     target_table: str,
#     pk_cols: list[str],
#     partition_keys: list[str],
#     partition_granularity: str,
#     cursor=None,
# ):
#     df = df.withColumn(
#         "_partition_key_base",
#         coalesce(*[col(c) for c in partition_keys]) # Coalesce the date columns first
#     )
#     if partition_granularity.upper() == "MONTH":
#         df = df.withColumn(
#             "_partition_key",
#             date_format(
#                 date_trunc("month", col("_partition_key_base")), # Then truncate to month
#                 "yyyy-MM-01"
#             )
#         )
#     elif partition_granularity.upper() == "DAY":
#         df = df.withColumn(
#             "_partition_key",
#             date_format(
#                 date_trunc("day", col("_partition_key_base")), # Then truncate to day),
#                 "yyyy-MM-dd"
#             )
#         )
#     else:
#         raise ValueError("partition_granularity must be either 'month' or 'day'")
    
#     full_table_identifier = sql.Identifier(target_schema, target_table)
#     temp_table_name = f"temp_{target_schema}_{target_table}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

#     partitions_to_replace = [row._partition_key for row in df.select("_partition_key").distinct().collect()]
#     logging.info(f"Partitions to be merged: {partitions_to_replace}")

#     try:
#         logging.info(f"Writing to temp table: {temp_table_name}...")
#         df.drop("_partition_key_base").write.jdbc(url=pg_url, table=temp_table_name, mode="overwrite")
#         logging.info(f"Successfully wrote to temporary table.")

#         all_columns = sql.SQL(",").join(sql.Identifier(c) for c in df.columns)
    
#         with transaction_context(cursor) as cur:
#             for part_start_date_str in partitions_to_replace:
#                 # --- 2. Write to Temporary Table ---
#                 if partition_granularity.upper() == "MONTH":
#                     partition_name = f"{target_table}_y{part_start_date_str[:4]}m{part_start_date_str[5:7]}"
#                     start_date = datetime.strptime(part_start_date_str, "%Y-%m-%d").date()
#                     end_date = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1)

#                 else:
#                     partition_name = f"{target_table}_y{part_start_date_str[:4]}m{part_start_date_str[5:7]}d{part_start_date_str[8:10]}"
#                     start_date = datetime.strptime(part_start_date_str, "%Y-%m-%d").date()
#                     end_date = start_date + timedelta(days=1)

#                 partition_identifier = sql.Identifier(target_schema, partition_name)
#                 logging.info(f"Processing partition: {partition_name} from {start_date} to {end_date}")

#                 # create the new partition
#                 cur.execute(sql.SQL("""
#                     CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF {parent_table}
#                     FOR VALUES FROM (%s) TO (%s)
#                 """).format(
#                     partition_name=partition_identifier,
#                     parent_table=full_table_identifier,
#                 ), (start_date, end_date))

#                 pk_columns_str = sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols)
#                 update_set_str = sql.SQL(", ").join(
#                     sql.SQL("{col} = excluded.{col}")
#                     .format(col=sql.Identifier(c)) for c in df.columns if c not in pk_cols
#                 )
#                 if not update_set_str:
#                     merge_sql = sql.SQL("""
#                         INSERT INTO {partition_name} ({all_columns})
#                         SELECT {all_columns} FROM {temp_table_name}
#                         WHERE _partition_key = (%s)
#                         ON CONFLICT ({pk_columns_str}) DO NOTHING
#                     """).format(
#                         partition_name=partition_identifier,
#                         all_columns=all_columns,
#                         temp_table_name=sql.Identifier(temp_table_name),
#                         pk_columns_str=pk_columns_str
#                     )
#                 else:
#                     merge_sql = sql.SQL("""
#                         INSERT INTO {partition_name} ({all_columns})
#                         SELECT {all_columns} FROM {temp_table_name}
#                         WHERE _partition_key = (%s)
#                         ON CONFLICT ({pk_columns_str}) DO UPDATE SET
#                             {update_set_str}
#                     """).format(
#                         partition_name=partition_identifier,
#                         all_columns=all_columns,
#                         temp_table_name=sql.Identifier(temp_table_name),
#                         pk_columns_str=pk_columns_str,
#                         update_set_str=update_set_str
#                     )

#                 logging.info(f"Executing merge SQL from {temp_table_name} to {full_table_identifier}...")
#                 cur.execute(merge_sql, part_start_date_str)
#                 logging.info("Merge operation complete.")

#     finally:
#         logging.info(f"Dropping temporary table {temp_table_name}...")
#         try:
#             with transaction_context(cursor) as cur:
#                 cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
#             logging.info("Temporary table dropped successfully.")
#         except Exception as e:
#             logging.error(f"Failed to drop temporary table {temp_table_name}: {e}", exc_info=True)

def load_fact_table_incrementally(
    spark: SparkSession,
    df: DataFrame,
    target_schema: str,
    target_table: str,
    pk_cols: list[str],
    cursor=None,
):
    full_table_identifier = sql.Identifier(target_schema, target_table)
    temp_table_name = f"temp_{target_schema}_{target_table}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    temp_table_identifier = sql.Identifier(temp_table_name)

    try:
        logging.info(f"Writing to temp table: {temp_table_name}...")
        df.drop("_partition_key_base").write.jdbc(url=pg_url, table=temp_table_name, mode="overwrite")
        logging.info(f"Successfully wrote to temporary table.")

        all_columns = sql.SQL(",").join(sql.Identifier(c) for c in df.columns)
    
        with transaction_context(cursor) as cur:
            cur.execute(sql.SQL("CREATE INDEX idx_temp_table ON {temp_table_name} ({pk_cols})").format(
                temp_table_name=temp_table_identifier,
                pk_cols=sql.SQL(", ").join(sql.Identifier(c) for c in pk_cols)
            ))

            join_condition = sql.SQL(" AND ").join(
                sql.SQL("t.{col} = s.{col}")
                .format(col=sql.Identifier(c)) for c in pk_cols
            )
            delete_sql = sql.SQL("""
                DELETE FROM {full_table_name} t
                USING {temp_table_name} s
                WHERE {join_condition}
            """).format(
                full_table_name=full_table_identifier,
                temp_table_identifier=temp_table_identifier,
                join_condition=join_condition
            )
            cur.execute(delete_sql)

            insert_sql = sql.SQL("""
                INSERT INTO {target_table} ({all_columns})
                SELECT ({all_columns}) FROM {temp_table_name}
            """).format(
                target_table=full_table_identifier,
                all_columns=all_columns,
                temp_table_name=temp_table_identifier
            )
            cur.execute(insert_sql)

    finally:
        logging.info(f"Dropping temporary table {temp_table_name}...")
        try:
            with transaction_context(cursor) as cur:
                cur.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
            logging.info("Temporary table dropped successfully.")
        except Exception as e:
            logging.error(f"Failed to drop temporary table {temp_table_name}: {e}", exc_info=True)


def load_fact_to_postgres(
    spark: SparkSession,
    df: DataFrame,
    target_schema: str,
    target_table: str,
    pk_cols: list[str] | None = None,
    partition_key: str | None = None,
    partition_granularity: str | None = None,
    cursor=None,
):
    # --- 1. Input Validation ---
    if not partition_granularity:
        raise ValueError("partition_granularity must be provided")
    if partition_granularity.upper() == "MONTH":
        df = df.withColumn("_partition_key", date_format(col(partition_key), "yyyy-MM-01"))
    elif partition_granularity.upper() == "DAY":
        df = df.withColumn("_partition_key", date_format(col(partition_key), "yyyy-MM-dd"))
    else:
        raise ValueError("partition_granularity must be either 'month' or 'day'")
    
    full_table_identifier = sql.Identifier(target_schema, target_table)
    temp_table_name = f"temp_{target_schema}_{target_table}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    partitions_to_replace = [row._partition_key for row in df.select("_partition_key").distinct().collect()]
    logging.info(f"Partitions to be replaced: {partitions_to_replace}")

    try:
        df.drop("_partition_key").write.jdbc(url=pg_url, table=temp_table_name, mode="overwrite", properties=pg_properties)

        with transaction_context(cursor) as cur:
            for part_start_date_str in partitions_to_replace:
                # --- 2. Write to Temporary Table ---
                if partition_granularity.upper() == "MONTH":
                    partition_name = f"{target_table}_y{part_start_date_str[:4]}m{part_start_date_str[5:7]}"
                    start_date = datetime.strptime(part_start_date_str, "%Y-%m-%d").date()
                    end_date = (start_date.replace(day=28) + timedelta(days=4)).replace(day=1)

                else:
                    partition_name = f"{target_table}_y{part_start_date_str[:4]}m{part_start_date_str[5:7]}d{part_start_date_str[8:10]}"
                    start_date = datetime.strptime(part_start_date_str, "%Y-%m-%d").date()
                    end_date = start_date + timedelta(days=1)

                partition_identifier = sql.Identifier(target_schema, partition_name)
                logging.info(f"Processing partition: {partition_name} from {start_date} to {end_date}")

                # remove existing partition
                cur.execute(sql.SQL("ALTER TABLE {parent_table} DETACH PARTITION {partition_name} CONCURRENTLY").format(
                    parent_table=full_table_identifier,
                    partition_name=partition_identifier
                ))
                # drop the old partition that needs to be replaced
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {partition_name};").format(
                    partition_name=partition_identifier
                ))

                # create the new partition
                cur.execute(sql.SQL("""
                    CREATE TABLE {partition_name} PARTITION OF {parent_table}
                    FOR VALUES FROM (%s) TO (%s)
                """).format(
                    partition_name=partition_identifier,
                    parent_table=full_table_identifier,
                ), (start_date, end_date))

            all_columns = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns if c != "_partition_key")
            logging.info("Inserting data into the main partitioned table...")
            cur.execute(sql.SQL("""
                INSERT INTO {parent_table} ({all_columns})
                SELECT {all_columns} FROM {temp_table_name};
            """).format(
                parent_table=full_table_identifier,
                all_columns=all_columns,
                temp_table_name=temp_table_name
            ))
            logging.info("Data insertion complete.")

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


def get_all_watermarks(spark: SparkSession) -> dict[str, datetime]:
    """
    Reads the entire watermark table from PostgreSQL in a single query and
    returns the results as a dictionary mapping table_name to its watermark.
    """
    watermark_table_name = "dwh_meta._dwh_watermarks"
    logging.info(f"Querying for all watermarks from {watermark_table_name}...")
    
    default_watermark = datetime(1970, 1, 1, tzinfo=timezone.utc)
    
    try:
        # This is very efficient as the watermark table is tiny.
        watermark_df = spark.read.jdbc(url=pg_url, table=watermark_table_name, properties=pg_properties)
        
        if watermark_df.rdd.isEmpty():
            logging.warning("Watermark table is empty. Returning empty dictionary.")
            return {}

        # Collect the few rows into the driver and convert to a dictionary for easy lookups
        watermark_rows = watermark_df.collect()
        watermarks = {row['table_name']: row['last_inserted_at'] for row in watermark_rows}
        
        logging.info(f"Successfully fetched {len(watermarks)} watermarks.")
        return watermarks
    
    except Exception as e:
        logging.warning(f"Could not read watermark table (it might not exist yet). Returning empty dictionary. Error: {e}")
        return {}


def write_watermarks(cursor, new_watermarks: dict[str, datetime]):
    """
    Efficiently upserts a batch of new watermark values into the metadata table
    using a single database command.

    Args:
        cursor: An active psycopg2 database cursor.
        new_watermarks: A dictionary mapping table_name to its new watermark timestamp.
    """
    if not new_watermarks:
        logging.info("No new watermarks to update. Skipping.")
        return

    logging.info(f"Bulk updating {len(new_watermarks)} watermarks...")
    
    # Prepare the data as a list of tuples for execute_values
    data_to_upsert = list(new_watermarks.items())
    
    update_sql = sql.SQL("""
        INSERT INTO dwh_meta._dwh_watermarks (table_name, last_inserted_at)
        VALUES %s
        ON CONFLICT (table_name) DO UPDATE SET
            last_inserted_at = EXCLUDED.last_inserted_at;
    """)
    
    # bulk operation
    execute_values(cursor, update_sql, data_to_upsert)
    logging.info("Bulk watermark update complete.")