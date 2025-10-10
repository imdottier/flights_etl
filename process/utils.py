import os
import logging
from pathlib import Path

import re
import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    pandas_udf, col, coalesce, to_timestamp, explode,
    sha2, concat_ws, current_timestamp, xxhash64, lit
)
import functools
from pyspark.sql.types import StructType,  StringType
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


def camel_to_snake(name: str) -> str:
    """Converts a single string from camelCase to snake_case."""
    if name is None:
        return None
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


def flatten_df(df: DataFrame, cols_to_snake_case_values: set[str] | None = None) -> DataFrame:
    if cols_to_snake_case_values is None:
        cols_to_snake_case_values = set()

    @pandas_udf(StringType())
    def camel_to_snake_vectorized(series: pd.Series) -> pd.Series:
        return series.str.replace(r'(?<!^)(?=[A-Z])', '_', regex=True).str.lower()
    
    flat_cols = []

    def get_flat_cols(schema: StructType, prefix: str = ""):
        for field in schema.fields:
            full_col_name = f"{prefix}.{field.name}" if prefix else field.name

            if isinstance(field.dataType, StructType):
                get_flat_cols(field.dataType, prefix=f"{full_col_name}")
            else:
                alias_name = camel_to_snake(full_col_name.replace(".", "_"))

                if alias_name in cols_to_snake_case_values and isinstance(field.dataType, StringType):
                    flat_cols.append(
                        camel_to_snake_vectorized(col(full_col_name)).alias(alias_name)
                    )
                else:
                    flat_cols.append(
                        col(full_col_name).alias(alias_name)
                    )

    try:
        get_flat_cols(df.schema)
        
        if not flat_cols:
            logging.warning("flatten_df resulted in no columns. Returning an empty DataFrame.")
            return df.sparkSession.createDataFrame([], schema=StructType([]))
            
        return df.select(flat_cols)

    except Exception as e:
        logging.error("An error occurred during DataFrame flattening.", exc_info=True)
        raise


def transform_timestamps(df: DataFrame) -> DataFrame:
    source_format = "yyyy-MM-dd HH:mm'Z'"
    iso_8601_format = "yyyy-MM-dd'T'HH:mm:ss'Z'"
    local_with_offset_format = "yyyy-MM-dd HH:mmXXX"

    utc_cols = [c for c in df.columns if c.endswith("_utc")]
    local_cols = [c for c in df.columns if c.endswith("_local")]

    for utc_col in utc_cols:
        df = df.withColumn(
            utc_col,
            coalesce(
                to_timestamp(col(utc_col), source_format),
                to_timestamp(col(utc_col), iso_8601_format),
            )
        )

    for local_col in local_cols:
        df = df.withColumn(
            local_col + "_ntz",
            to_timestamp(col(local_col), local_with_offset_format)
        )

    return df


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


def write_delta_table_with_scd(
    spark: SparkSession,
    df: DataFrame,
    db_name: str,
    table_name: str,
    business_keys: list[str],
    surrogate_key: str,
):
    full_table_name = f"{db_name}.{table_name}"
    table_path = f"/home/dottier/flights_etl/{db_name}/{table_name}"

    if not business_keys or surrogate_key is None:
        raise ValueError("Business keys and surrogate key must be provided for SCD Type 2 merge.")

    logging.info(f"--- Preparing to write to Delta table: {full_table_name} ---")

    now = current_timestamp()

    try:
        if not spark.catalog.tableExists(full_table_name):
            logging.info(f"Table {full_table_name} does not exist. Creating new Delta table.")

            df = df.withColumn(
                "effective_start_date", now
            ).withColumn(
                "effective_end_date", lit(None).cast("timestamp")
            )

            df = df.select(
                xxhash64(concat_ws("|", *business_keys, now)).alias(surrogate_key),
                *df.columns,
            )
            
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(table_path)

            logging.info(f"Registering Delta table {full_table_name} at location {table_path}.")
            spark.sql(f"CREATE TABLE {full_table_name} USING DELTA LOCATION '{table_path}'")
            logging.info(f"Table {full_table_name} created successfully.")

            return
        
        logging.info(f"Table {full_table_name} exists. Performing SCD Type 2 merge.")
        target_table = DeltaTable.forName(spark, full_table_name)

        non_key_columns = [col for col in df.columns if col not in business_keys]
        source_df_with_hash = df.withColumn(
            "row_hash",
            sha2(concat_ws("|", *non_key_columns), 256)
        )

        source_df_with_hash.printSchema()

        target_df_with_hash = (
            target_table.toDF()
            .filter(col("effective_end_date").isNull())
            .withColumn(
                "row_hash",
                sha2(concat_ws("|", *non_key_columns), 256)
            )
        )

        target_df_with_hash.printSchema()

        join_condition = [
            col(f"source.{key}") == col(f"target.{key}") for key in business_keys
        ]
        final_join_condition = functools.reduce(lambda x, y: x & y, join_condition) if join_condition else None

        staging_df = source_df_with_hash.alias("source").join(
            target_df_with_hash.alias("target"),
            on=final_join_condition,
            how="left"
        ).select(
            "source.*",
            col(f"target.{surrogate_key}").alias("existing_sk"),
            col("target.row_hash").alias("existing_hash")
        )

        expired_records = (
            staging_df
            .filter((col("existing_sk").isNotNull()) & (col("existing_hash") != col("row_hash")))
            .select(
                col("existing_sk").alias(surrogate_key),
                now.alias("effective_end_date")
            )
        )

        new_records = (
            staging_df
            .select(
                xxhash64(concat_ws("||", *business_keys, now)).alias(surrogate_key),
                *df.columns,
                now.alias("effective_start_date"),
                lit(None).cast("timestamp").alias("effective_end_date")
            )
        )

        final_change_set = new_records.unionByName(expired_records, allowMissingColumns=True)

        (
            target_table.alias("target")
            .merge(
                final_change_set.alias("source"),
                condition=f"target.{surrogate_key} = source.{surrogate_key}"
            )
            .whenNotMatchedInsertAll()
            .whenMatchedUpdate(
                set={"effective_end_date": "source.effective_end_date"}
            )
            .execute()
        )

        logging.info(f"SCD Type 2 merge into {full_table_name} completed successfully.")

    except Exception as e:
        logging.error(f"FATAL: Failed during WRITE phase for '{full_table_name}'. Error: {e}", exc_info=True)
        raise


def enrich_flights_data(
    bronze_flights: DataFrame,
):
    departure_exploded_df = (
        bronze_flights.select(
            explode("departures").alias("data"),
            "airport",
            "ingestion_hour",
        )
    )

    arrival_exploded_df = (
        bronze_flights.select(
            explode("arrivals").alias("data"),
            "airport",
            "ingestion_hour",
        )
    )