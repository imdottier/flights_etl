import os
import logging
from pathlib import Path

import re
import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    pandas_udf, col, coalesce, to_timestamp, explode,
    sha2, concat_ws, current_timestamp, xxhash64, lit,
    lower, trim, when, length, regexp_extract
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
    base_path = BRONZE_RAW_BASE / table_name
    print(base_path)

    if ingestion_hours:
        # Read selected hours
        paths = [str(base_path / f"ingestion_hour={hour}") for hour in ingestion_hours]
        logging.info(f"Reading from selected partitions: {paths}")
    else:
        # Read all partitions
        logging.info(f"Reading from all partitions in: {base_path}")
        paths = str(base_path)
    
    try:
        logging.info(f"Trying to read JSON from path '{paths}'")

        reader = spark.read.schema(schema).option("basePath", str(base_path))
        df = reader.json(paths)

        logging.info(f"Successfully read data into a DataFrame.")
        return df

    except Exception as e:
        logging.error(f"Failed to read JSON from path '{paths}'. Error: {e}", exc_info=True)
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
    write_mode: str, # 'overwrite_table', 'overwrite_partitions', 'merge'
    merge_keys: list[str] | None = None,
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

    # Safety checks
    if write_mode == "overwrite_partitions" and not partition_cols:
        raise ValueError("FATAL: 'partition_cols' must be provided for 'overwrite_partitions' mode.")
    if write_mode == "merge" and not merge_keys:
        raise ValueError("FATAL: 'merge_keys' must be provided for 'merge' mode.")

    try:
        table_exists = spark.catalog.tableExists(full_table_name)

        if not table_exists:
            logging.info(f"Table '{full_table_name}' does not exist. Creating new external table...")
            
            if partition_cols and write_mode in ["overwrite_table", "overwrite_partitions"]:
                logging.info(f"Creating and partitioning table by: {partition_cols}")
                (
                    df.write
                    .format("delta")
                    .mode("overwrite")
                    # DO NOT use overwriteSchema here, as it conflicts with partitionBy on creation
                    .partitionBy(*partition_cols)
                    .save(table_path)
                )

            # Case 2: The table will NOT be partitioned
            else:
                logging.info("Creating a new unpartitioned table.")
                (
                    df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true") # Safe to use here
                    .save(table_path)
                )

            spark.sql(f"CREATE TABLE {full_table_name} USING DELTA LOCATION '{table_path}'")
            logging.info(f"✅ Table '{full_table_name}' successfully created.")
            # Since the table was just created with the full data, the job is done.
            return

        # --- Logic for Full Table Overwrite ---
        if write_mode == "overwrite_table":
            logging.info(f"Performing a FULL table overwrite for {full_table_name}.")
            writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            
            # Partitioning is only applied on creation
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(table_path)
            # Ensure table is registered in metastore after the data is written
            spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA LOCATION '{table_path}'")

        # --- Logic for Dynamic Partition Overwrite ---
        elif write_mode == "overwrite_partitions":
            logging.info(f"Performing a DYNAMIC PARTITION overwrite for {full_table_name}.")
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
                .partitionBy(*partition_cols).save(table_path)
    
        # --- Logic for Merge ---
        elif write_mode == "merge":
            logging.info(f"Performing a MERGE operation for {full_table_name}.")
            delta_table = DeltaTable.forPath(spark, table_path)
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            
            delta_table.alias("target").merge(
                df.alias("source"), merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        else:
            raise ValueError(f"Invalid write_mode '{write_mode}'. Must be 'overwrite_table', 'overwrite_partitions', or 'merge'.")

        logging.info(f"✅ Write operation '{write_mode}' for {full_table_name} complete.")

    except Exception as e:
        logging.error(f"FATAL: Failed during WRITE phase for '{full_table_name}'.", exc_info=True)
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

        target_df_with_hash = (
            target_table.toDF()
            .filter(col("effective_end_date").isNull())
            .withColumn(
                "row_hash",
                sha2(concat_ws("|", *non_key_columns), 256)
            )
        )

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
            .filter((col("existing_sk").isNull()) | (col("existing_hash") != col("row_hash")))
            .select(
                xxhash64(concat_ws("||", *business_keys)).alias(surrogate_key),
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


from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, coalesce, concat_ws, explode, lit, lower, regexp_extract,
    trim, when, xxhash64, length
)

def enrich_flights_data(bronze_flights: DataFrame) -> DataFrame:
    """
    Enrich the bronze flights data by adding SKs (airport, flight, airline, aircraft),
    best scheduled time UTC, composite PK, and time zone info.

    Args:
        bronze_flights (DataFrame): The bronze flights data.

    Returns:
        DataFrame: Enriched flights DataFrame.
    """

    # --- Helper: build airport SK ---
    def airport_sk(iata, icao, name):
        return xxhash64(coalesce(iata, icao, lower(trim(name))))

    # --- Helper: flatten and timestamp-transform ---
    def preprocess(df, field):
        exploded = df.select(explode(field).alias("data"), "airport", "ingestion_hour")
        flat = flatten_df(exploded.select("data.*", "airport", "ingestion_hour"))
        return transform_timestamps(flat)

    # --- Departure flights ---
    dep_df = preprocess(bronze_flights, "departures").withColumns({
        "departure_airport_sk": xxhash64(col("airport")),
        "arrival_airport_sk": airport_sk(
            col("arrival_airport_iata"),
            col("arrival_airport_icao"),
            col("arrival_airport_name"),
        ),
    })

    # --- Arrival flights ---
    arr_df = preprocess(bronze_flights, "arrivals").withColumns({
        "departure_airport_sk": airport_sk(
            col("departure_airport_iata"),
            col("departure_airport_icao"),
            col("departure_airport_name"),
        ),
        "arrival_airport_sk": xxhash64(col("airport")),
    })

    # --- Common fields for both ---
    def enrich_common(df):
        return (
            df.withColumn(
                "best_scheduled_time_utc",
                coalesce(col("departure_scheduled_time_utc"), col("arrival_scheduled_time_utc")),
            )
            .withColumn(
                "flight_composite_pk",
                concat_ws(
                    "|",
                    col("best_scheduled_time_utc"),
                    col("number"),
                    col("departure_airport_sk"),
                    col("arrival_airport_sk"),
                ),
            )
            .withColumn("flight_sk", xxhash64(col("flight_composite_pk")))
        )

    dep_df = enrich_common(dep_df)
    arr_df = enrich_common(arr_df)

    # --- Merge and deduplicate ---
    flights = dep_df.union(arr_df).dropDuplicates(["flight_sk"])

    # --- Airline SK ---
    airline_code = when(
        (length(col("airline_name")).between(3, 4)) & col("airline_name").rlike("^[A-Z]+$"),
        col("airline_name"),
    ).otherwise(lower(trim(col("airline_name"))))

    flights = flights.withColumn(
        "airline_sk",
        xxhash64(coalesce(col("airline_iata"), col("airline_icao"), airline_code)),
    )

    # --- Aircraft SK + type ---
    flights = flights.withColumns({
        "aircraft_sk": xxhash64(
            when(
                col("aircraft_reg").isNull()
                & col("aircraft_mode_s").isNull()
                & col("aircraft_model").isNull(),
                lit("UNKNOWN"),
            ).otherwise(
                coalesce(col("aircraft_reg"), col("aircraft_mode_s"), col("aircraft_model"))
            )
        ),
        "aircraft_sk_type": when(col("aircraft_reg").isNotNull(), lit("reg"))
        .when(col("aircraft_mode_s").isNotNull(), lit("mode_s"))
        .when(col("aircraft_model").isNotNull(), lit("model"))
        .otherwise(lit("unknown")),
    })

    # --- Time zone info ---
    flights = flights.withColumns({
        "dep_local_timezone": regexp_extract(col("departure_scheduled_time_local"), r"([+-]\d{2}:\d{2}|Z)$", 1),
        "arr_local_timezone": regexp_extract(col("arrival_scheduled_time_local"), r"([+-]\d{2}:\d{2}|Z)$", 1),
    })

    return flights
