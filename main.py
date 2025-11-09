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
from process.silver_pipeline import run_silver_pipeline, run_openflights_pipeline

from load.publish_pipeline import run_publish_pipeline

def run_full_pipeline(
    ingestion_hours: list[str],
    process_detailed_dims: bool,
    skip_crawl: bool,
    run_aerodatabox: bool = True,
    run_openflights: bool = False,
):
    """
    Executes the entire data pipeline from crawl through silver.
    """
    try:
        batch_time = datetime.now(timezone.utc)
        logging.info(f"Batch runs at: {batch_time}")
        spark = get_spark_session()

        stage_counter = 1

        if run_aerodatabox:
            if not skip_crawl:
                logging.info(f">>> STAGE {stage_counter}: CRAWL <<<")
                run_crawl_pipeline(ingestion_hours=ingestion_hours, process_airports=process_detailed_dims)
                logging.info(f">>> CRAWL STAGE COMPLETED <<<")
            stage_counter += 1

            logging.info(f">>> STAGE {stage_counter}: BRONZE <<<")
            run_bronze_pipeline(spark=spark, ingestion_hours=ingestion_hours, process_airports=process_detailed_dims)
            logging.info(f">>> BRONZE STAGE COMPLETED <<<")
            stage_counter += 1

            logging.info(f">>> STAGE {stage_counter}: SILVER <<<")
            run_silver_pipeline(spark=spark, ingestion_hours=ingestion_hours, process_runways=process_detailed_dims, batch_time=batch_time)
            logging.info(f">>> SILVER STAGE COMPLETED <<<")
            stage_counter += 1

        if run_openflights:
            logging.info(f">>> STAGE {stage_counter}: OPENFLIGHTS <<<")
            run_openflights_pipeline(spark=spark)
            logging.info(f">>> OPENFLIGHTS STAGE COMPLETED <<<")
            stage_counter += 1

        # logging.info(f">>> STAGE {stage_counter}: PUBLISH <<<")
        # run_publish_pipeline(spark=spark, batch_time=batch_time)
        # logging.info(f">>> PUBLISH STAGE COMPLETED <<<")

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

    parser = argparse.ArgumentParser(
        description="Run the full AeroBox data pipeline.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        '--run-aerodatabox', 
        action='store_true',
        help="Run the main daily/hourly flights ETL pipeline."
    )
    parser.add_argument(
        '--run-openflights',
        action='store_true',
        help="Run the static data enrichment pipeline for OpenFlights data."
    )

    
    # Group arguments for 2 choices: explicit dates or a date range (inclusive)
    flights_parser = parser.add_argument_group('Flights Pipeline Options')
    date_group = flights_parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--dates",
        nargs='+',
        type=valid_date_format,
        metavar="YYYY-MM-DD-HH",
        help="Crawl an explicit list of batch timestamps (e.g. --dates 2023-01-01-00 2023-01-01-12 2023-01-02-00)"
    )

    date_group.add_argument(
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

    # For testing
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Show what would be executed without actually running any pipeline stages."
    )

    args = parser.parse_args()

    try:
        batches_to_process = []
        should_process_details = False
        
        if args.run_aerodatabox:
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

            if args.dry_run:
                logging.info(">>> DRY RUN MODE ENABLED <<<")
                logging.info(f"Target ingestion hours: {batches_to_process}")
                logging.info(f"Process detailed dimensions: {should_process_details}")
                logging.info(f"Skip crawl step: {args.skip_crawl}")
                logging.info("No stages will be executed.")
                exit(0)

        run_full_pipeline(
            ingestion_hours=batches_to_process,
            process_detailed_dims=should_process_details,
            skip_crawl=args.skip_crawl,
            run_aerodatabox=args.run_aerodatabox,
            run_openflights=args.run_openflights
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