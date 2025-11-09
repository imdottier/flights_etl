import os
import json
from pathlib import Path
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from datetime import datetime

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


def read_df(
    spark: SparkSession, layer: str, table_name: str,
    last_watermark: datetime = None, optional: bool = False, **kwargs
) -> DataFrame:
    full_table_name = f"{layer}.{table_name}"

    try:
        df = spark.read.table(full_table_name)
        if last_watermark:
            df = df.filter(col("_inserted_at") > lit(last_watermark))
        else:
            if "where" in kwargs:
                df = df.where(kwargs["where"])
        
        logging.info(f"Successfully read {full_table_name}")
        return df
    
    except Exception as e:
        if optional:
            logging.warning(f"Optional table {full_table_name} not found or unreadable: {e}")
            return None
        else:
            logging.error(f"Error reading {full_table_name}: {e}", exc_info=True)
            raise