import logging

from pyspark.sql import SparkSession

from utils.spark_session import get_spark_session
from utils.io_utils import read_df
from utils.logging_utils import setup_logging

from load.io_utils import (
    load_table_to_postgres, get_all_watermarks, write_watermarks,
    load_fact_to_postgres, load_fact_table_incrementally
)
from load.database import transaction_context
from utils.expression_utils import get_select_expressions

from load.utils import (
    read_silver_layer_data, create_derived_dimensions,
    load_dimensions, enrich_facts_with_dw_keys, calculate_new_watermarks,
    reconcile_airport_regions
)

from datetime import datetime

def run_publish_pipeline(
    spark: SparkSession,
    batch_time: datetime,
):
    """Orchestrates the Silver-to-Gold pipeline, publishing data to the data warehouse."""
    logging.info("="*50)
    logging.info("=== Starting the publish pipeline ===")
    logging.info("="*50)

    try:
        # --- 1. Read Existing Watermarks ---
        logging.info("Reading existing watermarks...")
        existing_watermarks = get_all_watermarks(spark)
        
        # --- 2. Read All Source Data from Silver Layer ---
        logging.info("Reading source data from Silver layer...")
        silver_dfs = read_silver_layer_data(spark, existing_watermarks)

        # --- 3. Synthesize New Dimension DataFrames in Spark ---
        logging.info("Creating derived dimension DataFrames...")
        derived_dims_dfs = create_derived_dimensions(silver_dfs["fct_flights"], batch_time)

        new_watermarks = calculate_new_watermarks(spark, silver_dfs)

        # Combine all dimension DataFrames for loading
        all_dims_to_load = {
            "dim_airports": silver_dfs["dim_airports"],
            "dim_runways": silver_dfs["dim_runways"],
            "dim_airlines": silver_dfs["dim_airlines"],
            "dim_aircrafts": silver_dfs["dim_aircrafts"],
            "dim_regions": silver_dfs["dim_regions"],
            **derived_dims_dfs # Adds the new flight_details and quality_combination dfs
        }

        # Reconcile iso_region for dim_airports
        if "dim_regions" in silver_dfs:
            dim_airports, dim_regions = reconcile_airport_regions(
                silver_dfs["dim_airports"], silver_dfs["dim_regions"]
            )

            all_dims_to_load["dim_airports"] = dim_airports
            all_dims_to_load["dim_regions"] = dim_regions

        # --- 4. Load All Dimensions Atomically to the Data Warehouse ---
        logging.info("Loading all dimension tables into the data warehouse...")
        with transaction_context() as master_cursor:
            load_dimensions(spark, master_cursor, all_dims_to_load)

        # --- 5. Enrich Fact Data with Warehouse Keys ---
        logging.info("Enriching fact data with newly loaded dimension keys...")
        gold_fct_df = enrich_facts_with_dw_keys(spark, silver_dfs["fct_flights"])

        with transaction_context() as master_cursor:
            # --- 6. Load the Final, Enriched Fact Table ---
            logging.info("Loading final Gold fact table...")
            load_table_to_postgres(
                spark=spark, df=gold_fct_df, target_schema="gold", target_table="fct_flights_intermediate",
                strategy="delete_insert", partition_key_col="flight_date", cursor=master_cursor,
                exclude_cols=["fact_flight_id"],
            )

            # --- 7. Update Watermarks ---
            logging.info("Updating watermarks...")
            write_watermarks(master_cursor, new_watermarks)
        
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
    run_publish_pipeline(spark)