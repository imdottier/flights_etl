import os
import json
from pathlib import Path
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame

load_dotenv()

base = Path(os.getenv("WORKSPACE_BASE", "./"))
BRONZE_RAW_BASE = base / "bronze_raw"


def write_json_to_bronze(data: dict, path: str, file_name: str):
    """Safely writes JSON data to the bronze layer."""
    file_path = None
    try:
        full_path = BRONZE_RAW_BASE / path
        full_path.mkdir(parents=True, exist_ok=True)
        file_path = full_path / file_name

        with open(file_path, "w") as f:
            json.dump(data, f, ensure_ascii=False)
        logging.info(f"Successfully wrote data to {file_path}")

    except (IOError, OSError) as e:
        logging.error(f"Failed to write JSON to {file_path}. Error: {e}")
        raise


def read_df(spark: SparkSession, layer: str, table_name: str, **kwargs) -> DataFrame:
    full_table_name = f"{layer}.{table_name}"

    try:
        df = spark.read.table(full_table_name)
        if "where" in kwargs:
            df = df.where(kwargs["where"])
        logging.info(f"Successfully read {full_table_name}")
        return df
    
    except Exception as e:
        logging.error(f"Error reading {full_table_name}: {e}", exc_info=True)
        raise