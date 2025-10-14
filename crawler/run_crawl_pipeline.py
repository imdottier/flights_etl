import argparse
import os
from dotenv import load_dotenv
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
import logging

from utils.logging_utils import setup_logging
from utils.io_utils import write_json_to_bronze

from crawler.api_utils import get_airport, get_flights
from crawler.time_utils import generate_batch_timestamps, valid_date_format


# --- ENVIRONMENT & CONFIG SETUP ---
load_dotenv()


base = Path(os.getenv("WORKSPACE_BASE", "./"))
BRONZE_RAW_BASE = base / "bronze_raw"
script_dir = BRONZE_RAW_BASE.parent
config_path = script_dir / "configs"
AIRPORT_PATH = config_path / "airports.json"


# --- main function (MODIFIED for historical runs) ---
def run_crawl_pipeline(ingestion_hours: list[str], process_airports: bool=False):
    """
    Main execution block for the crawl process.

    Args:
        ingestion_hours: List of ingestion hour strings (YYYY-MM-DD-HH) to process
        process_airports: Whether to fetch airport data as well
    """
    log_filepath = setup_logging(log_name="crawl")
    
    try:
        with open(AIRPORT_PATH, "r") as f:
            airports_to_process = json.load(f)
    except FileNotFoundError:
        logging.error(f"Airport config file not found at: {AIRPORT_PATH}")
        return
    
    # 1. Process airport data if requested
    if process_airports:
        logging.info("Starting airport data fetch...")

        airport_summary = {"processed": [], "failed": []}

        for airport in airports_to_process:
            iata = airport['iata']
            logging.info(f"Fetching data for {airport['airportName']} ({iata})")
            success = get_airport(iata_code=iata)

            if success:
                airport_summary["processed"].append(iata)
                logging.info(f"Successfully fetched data for {iata}")
            else:
                airport_summary["failed"].append(iata)
                logging.error(f"Failed to fetch data for {iata}")

        logging.info(f"Completed airport data fetch. Success: {len(airport_summary['processed'])}, Failed: {len(airport_summary['failed'])}")

    # 2. Process each batch sequentially
    for ingestion_hour in ingestion_hours:
        # Start time and summary are now PER BATCH
        batch_start_time = datetime.now(timezone.utc)
        crawl_summary = {
            "ingestion_hour": ingestion_hour,
            "batch_start_utc": batch_start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "log_file": str(log_filepath),
            "processed_airports": [],
            "failed_airports": []
        }

        # Log batch start
        logging.info(f"--- Starting batch for ingestion hour: {ingestion_hour} ---")
        try:
            for airport in airports_to_process:
                iata = airport['iata']
                logging.info(f"Fetching flights for {airport['airportName']} ({iata})")
                
                success = get_flights(iata_code=iata, ingestion_hour=ingestion_hour)

                if success:
                    crawl_summary["processed_airports"].append(iata)
                else:
                    crawl_summary["failed_airports"].append(iata)
        except Exception as e:
            logging.error(f"Error fetching flights for {ingestion_hour}: {e}")
            crawl_summary["error"] = str(e)

        # Finalize summary and write to file
        finally:
            batch_end_time = datetime.now(timezone.utc)
            duration_seconds = (batch_end_time - batch_start_time).total_seconds()

            crawl_summary["batch_end_utc"] = batch_end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            crawl_summary["duration_seconds"] = round(duration_seconds, 2)
            crawl_summary["total_success"] = len(crawl_summary["processed_airports"])
            crawl_summary["total_failed"] = len(crawl_summary["failed_airports"])

            summary_path = script_dir / "logs"
            summary_filename = f"summary_{ingestion_hour}.json"
            summary_filepath = summary_path / summary_filename

            with open(summary_filepath, "w") as f:
                json.dump(crawl_summary, f, indent=2)
            
            logging.info(f"--- Batch Finished for {ingestion_hour} ---")
            logging.info(f"Success: {crawl_summary['total_success']}, Failed: {crawl_summary['total_failed']}")
            logging.info(f"Crawl summary saved to: {summary_filepath}")
    

# if __name__ == "__main__":
#     # --- Use argparse to handle command-line arguments ---
#     parser = argparse.ArgumentParser(description="Fetch historical flight data from AeroDataBox.")

#     # Group arguments for 2 choices: explicit dates or a date range (inclusive)
#     group = parser.add_mutually_exclusive_group()

#     group.add_argument(
#         "--dates",
#         nargs='+',
#         type=valid_date_format,
#         metavar="YYYY-MM-DD-HH",
#         help="Crawl an explicit list of batch timestamps (e.g. --dates 2023-01-01-00 2023-01-01-12 2023-01-02-00)"
#     )

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
    
#     # Optional argument to process airport data (selected airports only)
#     parser.add_argument('--airports', action=argparse.BooleanOptionalAction)

#     args = parser.parse_args()

#     if args.end_date is None:
#         args.end_date = args.start_date

#     # Validation logic
#     # 1. Ensure that start-date and end-date are used together.
#     if args.start_date and not args.end_date:
#         parser.error("Argument --start-date requires --end-date.")
#     if args.end_date and not args.start_date:
#         parser.error("Argument --end-date requires --start-date.")
    
#     # 2. (Optional but good practice) Ensure the range is logical.
#     if args.start_date and args.end_date and args.start_date > args.end_date:
#         parser.error("--start-date cannot be after --end-date.")

#     # Execution logic
#     # 1. Determine if airport data should be processed
#     process_airports = args.airports

#     # 2. Generate the list of batches to process based on validated arguments.
#     ingestion_hours = generate_batch_timestamps(args)
#     logging.info(f"Generated {len(ingestion_hours)} batch(es) to process: {ingestion_hours}")

#     # 3. Call main with the list of batches and airport processing flag
#     run_crawl_pipeline(ingestion_hours, process_airports)