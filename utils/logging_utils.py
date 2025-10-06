import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

def setup_logging(log_name: str="test_logger", log_dir: str = "logs", level=logging.INFO):
    """
    Configures the root logger for an application run.
    This should be called once at the beginning of an application's entry point.

    Args:
        log_name (str): The base name for the log file (e.g., 'crawl', 'spark_bronze_to_silver').
        log_dir (str): The directory to save logs in.
        level: The logging level to set.
    """
    # Get the root logger.
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # IMPORTANT: Remove any existing handlers to prevent duplicate logs from previous runs in the same session.
    # This makes the function safe to call if needed for different configurations.
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # --- Create a unique, timestamped log file ---
    log_path = Path(log_dir).resolve()
    log_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"{log_name}_{timestamp}.log"
    log_filepath = log_path / log_filename

    # --- Create Handlers ---
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] - %(message)s")
    
    # File Handler
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Console Handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)
    
    logging.info(f"Logging configured. Log file: {log_filepath}")
    
    return log_filepath