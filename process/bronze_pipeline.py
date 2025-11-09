import argparse
import logging

from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from configs.schemas import flights_schema, full_airport_schema

from utils.spark_session import get_spark_session
from utils.logging_utils import setup_logging
from process.utils import json_to_df
from process.io_utils import write_delta_table

from datetime import datetime, timezone, timedelta


def run_bronze_pipeline(spark: SparkSession, ingestion_hours: list[str], process_airports: bool = False):
    """
    Runs the bronze pipeline for the given partitions.

    :param spark: The SparkSession object
    :param ingestion_hours: A list of partitions to run the pipeline for
    :param process_airports: Whether to process the airports data
    :return: None
    """
    spark.sql("DROP DATABASE IF EXISTS bronze CASCADE")
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    
    try:
        logging.info("Starting to process flights...")
        flights_df = json_to_df(
            spark=spark,
            table_name="flights", 
            schema=flights_schema,
            ingestion_hours=ingestion_hours
        )

        write_delta_table(
            spark=spark,
            df=flights_df,
            db_name="bronze",
            table_name="flights",
            write_mode="overwrite_partitions",
            partition_cols=["ingestion_hour"]
        )
        logging.info("Successfully wrote flights to Delta table.")

    except Exception as e:
        logging.error(f"Error processing flights: {e}", exc_info=True)
        raise

    if process_airports:
        try:
            logging.info("Starting to process airports...")
            airports_df = json_to_df(
                spark=spark,
                table_name="airports", 
                schema=full_airport_schema
            )

            write_delta_table(
                spark=spark,
                df=airports_df,
                db_name="bronze",
                table_name="airports",
                write_mode="overwrite_table"
            )
            logging.info("Successfully wrote airports to Delta table.")

        except Exception as e:
            logging.error(f"Error processing airports: {e}", exc_info=True)
            raise


# if __name__ == "__main__":
#     log_filepath = setup_logging(log_name="spark_run")
#     spark: SparkSession = get_spark_session()

#     parser = argparse.ArgumentParser(description="Run the Silver pipeline for specific date batches.")
    
#     group = parser.add_mutually_exclusive_group(required=False) # Make it required for testing
#     group.add_argument("--dates", nargs='+', help="A list of YYYY-MM-DD-HH partitions.")

#     group.add_argument(
#         "-s", "--start-date", 
#         type=valid_date_format, 
#         metavar="YYYY-MM-DD-HH", 
#         help="Start of a date range (inclusive)"
#     )
#     parser.add_argument(
#         "-e", "--end-date", 
#         type=valid_date_format,
#         metavar="YYYY-MM-DD-HH",
#         help="End of a date range (inclusive). Requires --start-date."
#     )

#     parser.add_argument('--airports', action=argparse.BooleanOptionalAction)

#     parser.add_argument(
#         '--skip-crawl', 
#         action='store_true',  # A simple flag. If present, it's True.
#         help="Skip the data crawling step and process only existing data in Bronze."
#     ) 

#     args = parser.parse_args()
#     if not args.dates and not args.start_date and not args.end_date:
#         logging.info("No date arguments provided. Reading all available partitions.")
#         batches_to_process = None
#     else:
#         batches_to_process = generate_batch_timestamps(args)
#         logging.info(f"Generated {len(batches_to_process)} batch(es) to process: {batches_to_process}")

#     run_bronze_pipeline(spark=spark, ingestion_hours=batches_to_process, process_airports=args.airports)