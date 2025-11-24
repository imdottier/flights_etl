import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

def setup_logging(log_name: str = "test_logger", log_dir: str = "logs", level=logging.INFO, log_to_file: bool = True):
    """
    Configures the root logger for an application run.

    Args:
        log_name (str): The base name for the log file.
        log_dir (str): The directory to save logs in.
        level: The logging level to set.
        log_to_file (bool): If True, creates a timestamped log file. If False, logs only to console.
    """
    # Get the root logger.
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # IMPORTANT: Remove any existing handlers to prevent duplicate logs
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # --- Common Formatter ---
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] - %(message)s")

    # --- Console Handler (Always Active) ---
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    log_filepath = None

    # --- File Handler (Optional) ---
    if log_to_file:
        try:
            log_path = Path(log_dir).resolve()
            log_path.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
            log_filename = f"{log_name}_{timestamp}.log"
            log_filepath = log_path / log_filename

            file_handler = logging.FileHandler(log_filepath)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            
            logging.info(f"Logging configured. Log file: {log_filepath}")
        except Exception as e:
            logging.error(f"Failed to setup file logging: {e}")
    else:
        logging.info("Logging configured (Console Only).")
    
    return log_filepath