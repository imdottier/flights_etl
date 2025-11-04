import logging
import argparse
from datetime import datetime, timezone
from contextlib import contextmanager
import functools

import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, date_trunc, date_format, 
    array_contains, format_string, max as spark_max, coalesce, lit, when
)
from delta.tables import DeltaTable

from load.io_utils import (
    load_table_to_postgres, read_df_from_postgres,
    write_watermarks,
)
from utils.io_utils import read_df
from utils.expression_utils import get_select_expressions

from sql.utils import get_sql

# ==============================================================================
# 2. PIPELINE-SPECIFIC HELPER FUNCTIONS
# ==============================================================================


def read_fact_data_for_overwrite(
    spark: SparkSession,
    target_schema: str,
    target_table: str,
    last_watermark: datetime
) -> DataFrame | None:
    """
    Performs a highly efficient, multi-step read on a partitioned Delta table.
    Includes robust error handling.
    
    1. Scans recent partitions to find which `ingestion_hour`s contain new data.
    2. Uses that list to read only the necessary partitions.
    3. Identifies the full set of business-date partitions to be overwritten.
    4. Reads all data from the SSoT for those business-date partitions.

    Returns a DataFrame containing the full data for all partitions to be overwritten.
    """
    
    silver_table_name = f"{target_schema}.{target_table}"
    logging.info(f"Starting efficient read for partitioned table {silver_table_name}...")

    # --- THE ONLY CHANGE IS WRAPPING YOUR LOGIC IN A TRY/EXCEPT BLOCK ---
    try:
        # --- Step 1: Partition Discovery ---
        logging.info(f"  Step 1: Discovering new ingestion_hour partitions since {last_watermark}...")
        
        # This read_df call is assumed to have its own try/except/raise
        new_ingestion_hours_df = read_df(spark, target_schema, target_table, last_watermark=last_watermark) \
            .select("ingestion_hour") \
            .distinct()

        # The .rdd.isEmpty() action can fail
        if new_ingestion_hours_df.rdd.isEmpty():
            logging.info("  No new data found based on watermark. Skipping.")
            return None

        # The .collect() action can fail
        new_ingestion_hours = [row.ingestion_hour for row in new_ingestion_hours_df.collect()]
        logging.info(f"  Found new data in ingestion_hour partitions: {new_ingestion_hours}")

        # --- Step 2: Read ONLY the relevant partitions from Silver ---
        logging.info("  Step 2: Reading only the affected partitions...")
        
        incremental_df = read_df(spark, target_schema, target_table, where_clause=f"ingestion_hour IN {tuple(new_ingestion_hours)}")
        
        # --- Step 3: Identify the full BUSINESS partitions to reload ---
        logging.info("  Step 3: Identifying affected business-date partitions...")
        partition_key_col = coalesce(col("dep_scheduled_at_utc"), col("arr_scheduled_at_utc"))
        
        affected_dates_df = incremental_df.select(
            date_trunc("day", partition_key_col).alias("flight_date")
        ).distinct()

        # The .collect() action can fail
        dates_to_reload = [row.flight_date for row in affected_dates_df.collect() if row.flight_date is not None]
        logging.info(f"  Business dates to be fully reloaded: {[d.strftime('%Y-%m-%d') for d in dates_to_reload]}")

        if not dates_to_reload:
            logging.warning("  New data was found, but it didn't map to any valid flight dates. Skipping.")
            return None

        # --- Step 4: Read the full data for the business partitions ---
        logging.info("  Step 4: Reading full data for affected business partitions from SSoT...")
        
        # Assumes read_df has its own try/except
        full_fct_df = read_df(spark, target_schema, target_table).withColumn(
            "flight_date", date_trunc("day", partition_key_col)
        )

        df_to_load = full_fct_df.where(col("flight_date").isin(dates_to_reload))
        logging.info(f"  Successfully prepared {df_to_load.count()} rows for overwrite.")
        
        return df_to_load

    except Exception as e:
        logging.error(
            f"FATAL: An error occurred during the read process for {silver_table_name}. Cannot proceed. Error: {e}", 
            exc_info=True
        )
        raise


def read_silver_layer_data(spark: SparkSession, watermarks: dict[str, datetime]) -> dict[str, DataFrame]:
    """Reads all necessary tables from the Silver layer and returns them in a dictionary."""

    # Old logic was to merge for dim tables so I did this, no longer needed for truncate load
    last_watermarks = {
        key: watermarks.get(f"silver.{key}", datetime(1970, 1, 1, tzinfo=timezone.utc))
        for key in ["dim_airports", "dim_runways", "dim_airlines", "dim_aircrafts", "fct_flights"]
    }

    # Read data from Silver
    dim_airports = read_df(spark, "silver", "dim_airports").drop("_data_hash")
    dim_runways = read_df(spark, "silver", "dim_runways").drop("_data_hash")
    dim_airlines = read_df(spark, "silver", "dim_airlines").drop("_data_hash")
    dim_aircrafts = read_df(spark, "silver", "dim_aircrafts").drop("_data_hash")
    fct_flights = read_fact_data_for_overwrite(
        spark, "silver", "fct_flights", last_watermarks["fct_flights"]
    )

    return {
        "dim_airports": dim_airports,
        "dim_runways": dim_runways,
        "dim_airlines": dim_airlines,
        "dim_aircrafts": dim_aircrafts,
        "fct_flights": fct_flights,
    }


def create_derived_dimensions(silver_flights_df: DataFrame, batch_time) -> dict[str, DataFrame]:
    """Creates the junk and combination dimension DataFrames from the fact data."""
    logging.info("Synthesizing dim_flight_details...")
    dim_flight_details_df = silver_flights_df.select(
        "flight_status", "codeshare_status", "is_cargo"
    ).distinct()

    dim_flight_details_df = dim_flight_details_df.withColumn( # Add the metadata columns
        "_ingested_at", lit(batch_time)
    ).withColumn(
        "_inserted_at", current_timestamp()
    )

    logging.info("Synthesizing dim_quality_combination...")
    dim_quality_combination_df = silver_flights_df.select(
        array_contains("dep_quality", "Basic").alias("dep_has_basic"),
        array_contains("dep_quality", "Live").alias("dep_has_live"),
        array_contains("arr_quality", "Basic").alias("arr_has_basic"),
        array_contains("arr_quality", "Live").alias("arr_has_live"),
    ).distinct()

    dim_quality_combination_df = dim_quality_combination_df.withColumn(
        "quality_desc",
        format_string(
            "Dep B:%s L:%s, Arr B:%s L:%s",
            col("dep_has_basic"), col("dep_has_live"),
            col("arr_has_basic"), col("arr_has_live")
        )
    ).withColumn( # Add the metadata columns
        "_ingested_at", lit(batch_time)
    ).withColumn(
        "_inserted_at", current_timestamp()
    )
    
    return {
        "dim_flight_details": dim_flight_details_df,
        "dim_quality_combination": dim_quality_combination_df
    }
    

def get_unknown_record_sql(table_name: str) -> str:
    """Returns the SQL to insert the 'Unknown' record for a given dimension table."""
    if table_name == "dim_airports":
        insert_sql = get_sql("dim_airports.sql")
        return insert_sql
    if table_name == "dim_runways":
        insert_sql = get_sql("dim_runways.sql")
        return insert_sql
    # Add other 'Unknown' records for other dimensions as needed
    return "" # Return empty string if no unknown record is defined


def load_dimensions(spark: SparkSession, cursor, dims_to_load: dict[str, DataFrame]):
    """Loads multiple dimension tables and their 'Unknown' records within a single transaction."""
    dim_configs = {
        "dim_runways": {"pk": ["runway_version_key"]},
        "dim_airports": {"pk": ["airport_sk"]},
        "dim_airlines": {"pk": ["airline_sk"]},
        "dim_aircrafts": {"pk": ["aircraft_sk"]},
        "dim_flight_details": {"pk": ["flight_status", "codeshare_status", "is_cargo"]},
        "dim_quality_combination": {"pk": ["dep_has_basic", "dep_has_live", "arr_has_basic", "arr_has_live"]},
    }

    for name, df in dims_to_load.items():
        if name in dim_configs:
            logging.info(f"Loading dimension: {name}")
            load_table_to_postgres(
                spark=spark, df=df, target_schema="gold", target_table=name,
                strategy="upsert", pk_cols=dim_configs[name]["pk"], cursor=cursor
            )

            unknown_sql = get_unknown_record_sql(name)
            if unknown_sql:
                cursor.execute(unknown_sql)


def enrich_facts_with_dw_keys(spark: SparkSession, silver_flights_df: DataFrame) -> DataFrame:
    """Performs the full transformation of the silver fact data to the gold, load-ready state."""
    logging.info("Reading dimension lookup tables from data warehouse...")
    quality_lookup_df = read_df_from_postgres(spark, "gold", "dim_quality_combination")
    flight_details_lookup_df = read_df_from_postgres(spark, "gold", "dim_flight_details")

    logging.info("Enriching fact data with new dimension keys...")
    fct_flights_df = silver_flights_df.withColumn(
        "dep_has_basic", array_contains(col("dep_quality"), "Basic")
    ).withColumn(
        "dep_has_live", array_contains(col("dep_quality"), "Live")
    ).withColumn(
        "arr_has_basic", array_contains(col("arr_quality"), "Basic")
    ).withColumn(
        "arr_has_live", array_contains(col("arr_quality"), "Live")
    )

    fct_flights_df = fct_flights_df.join(
        flight_details_lookup_df.select("flight_details_sk", "flight_status", "codeshare_status", "is_cargo"),
        on=["flight_status", "codeshare_status", "is_cargo"], how="left"
    ).join(
        quality_lookup_df.select("quality_combo_sk", "dep_has_basic", "dep_has_live", "arr_has_basic", "arr_has_live"),
        on=["dep_has_basic", "dep_has_live", "arr_has_basic", "arr_has_live"], how="left"
    )

    # Coalesce NULL foreign keys to -1
    # Fill numeric keys
    fct_flights_df = fct_flights_df.fillna({
        "flight_details_sk": -1,
        "quality_combo_sk": -1
    })

    # Fill SHA2 text keys
    fct_flights_df = (
        fct_flights_df
        .withColumn("departure_runway_version_key", when(col("departure_runway_version_key").isNull(), sha2(lit("-1"), 256)).otherwise(col("departure_runway_version_key")))
        .withColumn("arrival_runway_version_key", when(col("arrival_runway_version_key").isNull(), sha2(lit("-1"), 256)).otherwise(col("arrival_runway_version_key")))
    )

    select_exprs = get_select_expressions("gold", "fct_flights_intermediate")
    final_df = fct_flights_df.select(*select_exprs)

    return final_df


def calculate_new_watermarks(spark: SparkSession, source_dfs: dict[str, DataFrame]) -> dict[str, datetime]:
    """
    Calculates the maximum _inserted_at timestamp for each DataFrame in the input dictionary.
    
    Returns:
        A dictionary mapping the full table identifier (e.g., 'silver.dim_airports') 
        to its new high-watermark timestamp.
    """
    new_watermarks = {}
    logging.info("Calculating new watermarks from processed DataFrames...")
    for table_name, df in source_dfs.items():
        if not df.rdd.isEmpty():
            table_identifier = f"silver.{table_name}"
            
            # This is the Spark computation
            new_max = df.agg(spark_max("_inserted_at")).first()[0]
            
            if new_max:
                new_watermarks[table_identifier] = new_max
                logging.info(f"  New watermark for {table_identifier}: {new_max}")
    return new_watermarks
