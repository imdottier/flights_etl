import argparse
import requests
import os
from dotenv import load_dotenv
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
import logging
import sys

from utils.logging_utils import setup_logging
from utils.io_utils import write_json_to_bronze


# --- ENVIRONMENT & CONFIG SETUP ---
load_dotenv()

aerodatabox_api_key = os.getenv("AERODATABOX_API_KEY")
if not aerodatabox_api_key:
    logging.error("API_KEY not found in environment variables. Please check your .env file.")
    sys.exit(1) # Exit if the key is missing

headers = {
    "x-api-market-key": aerodatabox_api_key,
    "Content-Type": "application/json"
}


base = Path(os.getenv("WORKSPACE_BASE", "./"))
BRONZE_RAW_BASE = base / "bronze_raw"
script_dir = BRONZE_RAW_BASE.parent
config_path = script_dir / "configs"
AIRPORT_PATH = config_path / "airports.json"
BASE_URL = "https://prod.api.market/api/v1/aedbx/aerodatabox"

API_BATCH_DURATION_MINUTES = 12 * 60 # 720


def get_flights(iata_code: str, ingestion_hour: str) -> bool:
    """Fetches and writes flight data for a given airport. Returns True on success, False on failure."""
    url = f"{BASE_URL}/flights/airports/Iata/{iata_code}"
    
    try:
        current_time_for_api = datetime.now(timezone.utc)
        
        # Get last 12h mark
        target_batch_mark = datetime.strptime(ingestion_hour, "%Y-%m-%d-%H").replace(tzinfo=timezone.utc)

        start_time = target_batch_mark - timedelta(hours=12)
        
        # And the offset mins (mins from mark to now) for api
        offset_minutes = int((start_time - current_time_for_api).total_seconds() // 60)
        path = f"flights/ingestion_hour={ingestion_hour}/airport={iata_code}"

        params = {
            "offsetMinutes": offset_minutes,
            "durationMinutes": API_BATCH_DURATION_MINUTES,
            "withCodeshared": "false",
            "withLeg": "true",
            "withCargo": "true",
            "withPrivate": "true",
            "withLocation": "true"
        }

        # api call
        response = requests.get(url, params=params, headers=headers, timeout=5)
        response.raise_for_status()

        data = response.json()
        write_json_to_bronze(data, path, "payload.json")
        return True

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error for {iata_code}: {e.response.status_code} {e.response.reason}. URL: {e.response.url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Network Error for {iata_code}: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON response for {iata_code}. Response text: {response.text[:200]}...") # Log first 200 chars
    except Exception as e:
        logging.error(f"An unexpected error occurred for {iata_code}: {e}")

    return False


def get_airport(iata_code: str) -> bool:
    """Fetches and writes flight data for a given airport. Returns True on success, False on failure."""
    url = f"{BASE_URL}/airports/Iata/{iata_code}"
    
    try:
        path = f"airports/airport={iata_code}"

        params = {
            "withRunways": "true",
            "withTime": "true",
        }

        # api call
        response = requests.get(url, params=params, headers=headers, timeout=5)
        response.raise_for_status()

        data = response.json()
        write_json_to_bronze(data, path, "payload.json")
        return True

    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error for {iata_code}: {e.response.status_code} {e.response.reason}. URL: {e.response.url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Network Error for {iata_code}: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON response for {iata_code}. Response text: {response.text[:200]}...") # Log first 200 chars
    except Exception as e:
        logging.error(f"An unexpected error occurred for {iata_code}: {e}")

    return False


def valid_date_format(s: str) -> datetime:
    """
    Custom argparse type for validating date strings.
    Ensures format is YYYY-MM-DD-HH and hour is a valid batch start (00 or 12).
    """
    try:
        dt = datetime.strptime(s, "%Y-%m-%d-%H")
        if dt.hour not in [0, 12]:
            raise argparse.ArgumentTypeError(
                f"Invalid hour in date string: {s}. Hour must be 00 or 12."
            )
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date string: {s}. Expected format: YYYY-MM-DD-HH"
        )
    

def generate_batch_timestamps(args) -> list[str]:
    """
    Generate a list of batch timestamps based on the provided start and end dates.
    """
    if args.dates:
        return [dt.strftime("%Y-%m-%d-%H") for dt in args.dates]
    
    if args.start_date:
        timestamps = []
        current_batch = args.start_date
        while current_batch <= args.end_date:
            timestamps.append(current_batch.strftime("%Y-%m-%d-%H"))
            current_batch += timedelta(hours=12)
        return timestamps
    
    now = datetime.now(timezone.utc)
    if now.hour < 12:
        latest_batch_mark = now.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        latest_batch_mark = now.replace(hour=12, minute=0, second=0, microsecond=0)
    
    return [latest_batch_mark.strftime("%Y-%m-%d-%H")]


# --- main function (MODIFIED for historical runs) ---
def main(batches_to_process: list[str], process_airports: bool=False):
    """
    Main execution block for the crawl process.

    Args:
        batches_to_process: List of ingestion hour strings (YYYY-MM-DD-HH) to process
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
    for ingestion_hour in batches_to_process:
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
    

if __name__ == "__main__":
    # --- Use argparse to handle command-line arguments ---
    parser = argparse.ArgumentParser(description="Fetch historical flight data from AeroDataBox.")

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
    
    # Optional argument to process airport data (selected airports only)
    parser.add_argument('--airports', action=argparse.BooleanOptionalAction)

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
    process_airports = args.airports

    # 2. Generate the list of batches to process based on validated arguments.
    batches_to_process = generate_batch_timestamps(args)
    logging.info(f"Generated {len(batches_to_process)} batch(es) to process: {batches_to_process}")

    # 3. Call main with the list of batches and airport processing flag
    main(batches_to_process, process_airports)