import argparse
from datetime import datetime, timezone, timedelta
import logging


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
    if not args.dates and not args.start_date and not args.end_date:
        logging.info("No date arguments provided. Reading all available partitions.")
        return None

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