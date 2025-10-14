import argparse
import requests
import os
from dotenv import load_dotenv
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
import logging
import sys

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