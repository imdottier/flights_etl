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
    load_dimensions, calculate_new_watermarks,
    reconcile_airport_regions
)

from datetime import datetime

def run_publish_pipeline(
    spark: SparkSession,
):
    """Orchestrates the Silver-to-Staging pipeline, publishing data to the data warehouse."""
    logging.info("="*50)
    logging.info("=== Starting the publish pipeline ===")
    logging.info("="*50)

    try:
        # --- 1. Read Existing Watermarks ---
        logging.info("Reading existing watermarks...")
        existing_watermarks = get_all_watermarks(spark)
        
        logging.info("Reading source data from Silver layer...")
        silver_dfs = read_silver_layer_data(spark, existing_watermarks)
        new_watermarks = calculate_new_watermarks(spark, silver_dfs)

        # --- 2. Define the mapping from Silver to Staging tables ---
        tables_to_load = {
            # Silver DataFrame Key : Staging Table Name
            "dim_regions": "regions",
            "dim_airports": "airports",
            "dim_runways": "runways",
            "dim_airlines": "airlines",
            "dim_aircrafts": "aircrafts",
            "fct_flights": "flights", # Facts are treated the same!
        }

        # --- 3. Load ALL tables into Staging within a single transaction context ---
        logging.info("Loading all new data into the staging schema...")
        with transaction_context() as master_cursor:
            for silver_key, staging_table in tables_to_load.items():
                if silver_key in silver_dfs and silver_dfs[silver_key].count() > 0:
                    logging.info(f"Loading data for '{silver_key}' into 'stg.{staging_table}'")
                    load_table_to_postgres(
                        spark=spark,
                        df=silver_dfs[silver_key],
                        target_schema="stg",
                        target_table=staging_table,
                        cursor=master_cursor
                    )
                else:
                    logging.info(f"No new data for '{silver_key}'. Skipping.")

            # --- 4. Update Watermarks ---
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