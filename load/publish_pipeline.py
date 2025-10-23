import logging

from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, concat, lit, col, array_join, format_string

from utils.spark_session import get_spark_session
from utils.io_utils import read_df
from utils.logging_utils import setup_logging
from process.utils import json_to_df, write_delta_table

from load.io_utils import load_table_to_postgres, read_df_from_postgres
from load.database import transaction_context
from utils.expression_utils import get_select_expressions

from load.utils import (
    read_silver_layer_data, create_derived_dimensions,
    load_dimensions, enrich_facts_with_dw_keys
)

def publish_pipeline(
    spark: SparkSession,
    ingestion_hours: list[str] | None = None,
):
    """Orchestrates the Silver-to-Gold pipeline, publishing data to the data warehouse."""
    logging.info("="*50)
    logging.info("=== Starting the publish pipeline ===")
    logging.info("="*50)

    try:
        # --- 1. Read All Source Data from Silver Layer ---
        logging.info("Reading source data from Silver layer...")
        silver_dfs = read_silver_layer_data(spark, ingestion_hours)

        # --- 2. Synthesize New Dimension DataFrames in Spark ---
        logging.info("Creating derived dimension DataFrames...")
        derived_dims_dfs = create_derived_dimensions(silver_dfs["fct_flights"])

        # Combine all dimension DataFrames for loading
        all_dims_to_load = {
            "dim_airports": silver_dfs["dim_airports"],
            "dim_runways": silver_dfs["dim_runways"],
            "dim_airlines": silver_dfs["dim_airlines"],
            "dim_aircrafts": silver_dfs["dim_aircrafts"],
            **derived_dims_dfs # Adds the new flight_details and quality_combination dfs
        }

        # --- 3. Load All Dimensions Atomically to the Data Warehouse ---
        logging.info("Loading all dimension tables into the data warehouse...")
        with transaction_context() as master_cursor:
            load_dimensions(spark, master_cursor, all_dims_to_load)

        # --- 4. Enrich Fact Data with Warehouse Keys ---
        logging.info("Enriching fact data with newly loaded dimension keys...")
        gold_fct_df = enrich_facts_with_dw_keys(spark, silver_dfs["fct_flights"])

        # --- 5. Load the Final, Enriched Fact Table ---
        logging.info("Loading final Gold fact table...")
        with transaction_context() as master_cursor:
            load_table_to_postgres(
                spark=spark, 
                df=gold_fct_df,
                target_schema="gold",
                target_table="fct_flights_intermediate",
                strategy="delete_insert", 
                partition_key_col="ingestion_hour", 
                cursor=master_cursor
            )
        
        logging.info("✅ Publish pipeline completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
        raise
    finally:
        logging.info("="*50)
        logging.info("=== Finished the publish pipeline ===")
        logging.info("="*50)


if __name__ == "__main__":
    setup_logging()
    spark = get_spark_session()
    publish_pipeline(spark)