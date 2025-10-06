import os
import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

from dotenv import load_dotenv

load_dotenv()


base = Path(os.getenv("WORKSPACE_BASE", "./"))
BRONZE_RAW_BASE = base / "bronze_raw"

def json_to_df(
    spark: SparkSession,
    table_name: str,
    schema: StructType,
    ingestion_hours: list[str] | None = None,
):
    """
    Reads JSON files from a given path and enforce a default schema to it

    Args:
        spark: The active SparkSession.
        table_name: The name of the table to read from.
        schema: The schema to enforce on the read DataFrame.
        ingestion_hours: The list of ingestion hours to read from. If None, all partitions will be read.

    Returns:
        DataFrame: The DataFrame read from the given path.
    """
    base_path = os.path.join(BRONZE_RAW_BASE, table_name)

    if ingestion_hours:
        # Read selected hours
        path_glob = f"{base_path}/ingestion_hour={{{','.join(ingestion_hours)}}}"
        logging.info(f"Reading from selected partitions: {path_glob}")
        path_to_read = path_glob
    else:
        # Read all partitions
        logging.info(f"Reading from all partitions in: {base_path}")
        path_to_read = base_path
    
    try:
        logging.info(f"Trying to read JSON from path '{path_to_read}'")

        reader = spark.read.schema(schema)
        df = reader.json(path_to_read)

        logging.info(f"Successfully read data into a DataFrame.")
        return df

    except Exception as e:
        logging.error(f"Failed to read JSON from path '{path_to_read}'. Error: {e}", exc_info=True)
        raise


def write_delta_table(
    spark: SparkSession,
    df: DataFrame,
    db_name: str,
    table_name: str,
    write_mode: str = "overwrite",
    primary_keys: list[str] | None = None,
    partition_cols: list[str] | None = None,
):
    """
    Writes a DataFrame to a Delta table using dynamic partition overwrite.
    Creates the table with specified partitioning if it does not exist.

    Args:
        spark: The active SparkSession.
        df: The DataFrame to write.
        db_name: The database name (e.g., 'bronze', 'silver').
        table_name: The name of the target table.
        partition_cols: A list of column names to partition the table by (only used on creation).
    """
    full_table_name = f"{db_name}.{table_name}"
    table_path = f"{base}/{db_name}/{table_name}"
    logging.info(f"--- Preparing to write to Delta table: {full_table_name} ---")

    try:
        if not spark.catalog.tableExists(full_table_name):
            logging.info(f"Table '{full_table_name}' does not exist. Creating new external table...")

            logging.info(f"Writing delta table to {table_path}")
            
            writer = df.write.mode("overwrite") \
                        .format("delta") \
                        .option("mergeSchema", "true")

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            writer.save(table_path)

            logging.info(f"Registering table '{full_table_name}' in metastore.")
            spark.sql(f"CREATE TABLE {full_table_name} USING DELTA LOCATION '{table_path}'")
            logging.info(f"✅ EXTERNAL Table '{full_table_name}' successfully created and registered.")

        else:
            if write_mode == "merge":
                logging.info(f"Table '{full_table_name}' exists. Performing merge operation...")
                delta_table = DeltaTable.forName(spark, full_table_name)
                
                # Assuming 'id' is the primary key for merge condition; adjust as necessary
                merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys]) if primary_keys else "1=0"
                
                (
                    delta_table.alias("target").merge(
                        df.alias("source"),
                        merge_condition
                    ).whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            
                logging.info(f"✅ Merge operation for '{full_table_name}' complete.")
            
            elif write_mode == "overwrite":
                logging.info(f"Table '{full_table_name}' exists. Performing dynamic partition overwrite...")
                spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                
                df.write.mode("overwrite") \
                    .format("delta") \
                    .option("mergeSchema","true") \
                    .insertInto(full_table_name)

                logging.info(f"✅ Dynamic partition overwrite for '{full_table_name}' complete.")

    except Exception as e:
        logging.error(f"FATAL: Failed during WRITE phase for '{full_table_name}'. Error: {e}", exc_info=True)
        raise