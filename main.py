import argparse
import os
from dotenv import load_dotenv
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
import logging

from utils.logging_utils import setup_logging
from utils.spark_session import get_spark_session

from crawler.time_utils import generate_batch_timestamps, valid_date_format
from crawler.run_crawl_pipeline import run_crawl_pipeline
from process.bronze_pipeline import run_bronze_pipeline
from process.silver_pipeline import run_silver_pipeline


def run_full_pipeline(ingestion_hours: list[str], process_detailed_dims: bool, skip_crawl: bool):
    """
    Executes the entire data pipeline from crawl through silver.
    """
    try:
        # --- Stage 1: Crawl ---
        if not skip_crawl:
            logging.info(">>> STAGE 1: CRAWL (EXECUTING) <<<")
            run_crawl_pipeline(ingestion_hours=ingestion_hours, process_airports=process_detailed_dims)
            logging.info(">>> CRAWL STAGE COMPLETED. <<<")
        else:
            logging.info(">>> STAGE 1: CRAWL (SKIPPED) <<<")

        # --- Stage 2 & 3: Spark Processing (Bronze & Silver) ---
        logging.info("Initializing Spark session for data processing...")
        spark = get_spark_session()
        
        logging.info(">>> STAGE 2: BRONZE <<<")
        run_bronze_pipeline(spark=spark, ingestion_hours=ingestion_hours, process_airports=process_detailed_dims)
        logging.info(">>> BRONZE STAGE COMPLETED. <<<")

        logging.info(">>> STAGE 3: SILVER <<<")
        # Use the same, clear flag name
        run_silver_pipeline(spark=spark, ingestion_hours=ingestion_hours, process_runways=process_detailed_dims)
        logging.info(">>> SILVER STAGE COMPLETED. <<<")

    finally:
        # Ensure Spark is stopped even if a stage fails
        if 'spark' in locals() and spark:
            logging.info("Stopping Spark session.")
            spark.stop()


if __name__ == "__main__":
    """
    Main execution block for the crawl and process pipelines.
    """
    logger = setup_logging(log_name="pipeline_run")
    logging.info("==================================================")
    logging.info(f"=== STARTING FULL PIPELINE RUN ===")
    logging.info("==================================================")

    parser = argparse.ArgumentParser(description="Run the full AeroBox data pipeline.")
    # Group arguments for 2 choices: explicit dates or a date range (inclusive)
    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "--dates",
        nargs='+',
        type=valid_date_format,
        metavar="YYYY-MM-DD-HH",
        help="Crawl an explicit list of batch timestamps (e.g. --dates 2023-01-01-00 2023-01-01-12 2023-01-02-00)"
    )

    group.add_argument(
        "-s", "--start-date", 
        type=valid_date_format, 
        metavar="YYYY-MM-DD-HH", 
        help="Start of a date range (inclusive)"
    )
    parser.add_argument(
        "-e", "--end-date", 
        type=valid_date_format,
        metavar="YYYY-MM-DD-HH",
        help="End of a date range (inclusive). Requires --start-date."
    )
    
    parser.add_argument(
        '--skip-crawl', 
        action='store_true',  # A simple flag. If present, it's True.
        help="Skip the data crawling step and process only existing data in Bronze."
    )
    # Optional argument to process airport data (selected airports only)
    parser.add_argument('--process-details', action=argparse.BooleanOptionalAction)

    args = parser.parse_args()

    if args.end_date is None:
        args.end_date = args.start_date

    # Validation logic
    # 1. Ensure that start-date and end-date are used together.
    if args.start_date and not args.end_date:
        parser.error("Argument --start-date requires --end-date.")
    if args.end_date and not args.start_date:
        parser.error("Argument --end-date requires --start-date.")
    
    # 2. (Optional but good practice) Ensure the range is logical.
    if args.start_date and args.end_date and args.start_date > args.end_date:
        parser.error("--start-date cannot be after --end-date.")

    # Execution logic
    # 1. Determine if airport data should be processed
    should_process_details = args.process_details if args.process_details is not None else False

    # 2. Generate the list of batch timestamps
    batches_to_process = generate_batch_timestamps(args)
    logging.info(f"Target ingestion hours for this run: {batches_to_process}")
    logging.info(f"Process detailed dimensions: {should_process_details}")
    logging.info(f"Skip crawl step: {args.skip_crawl}")

    # 3. Call main with the list of batches and airport processing flag
    try:
        run_full_pipeline(
            ingestion_hours=batches_to_process, 
            process_detailed_dims=should_process_details,
            skip_crawl=args.skip_crawl
        )
        logging.info("==================================================")
        logging.info("=== FULL PIPELINE COMPLETED SUCCESSFULLY ===")
        logging.info("==================================================")
    except Exception as e:
        # The detailed error is already logged inside the functions.
        # This is the final catch-all.
        logging.critical("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        logging.critical("!!! FULL PIPELINE FAILED !!!")
        logging.critical("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        # Re-raise the exception to ensure the script exits with a non-zero status code
        raise

