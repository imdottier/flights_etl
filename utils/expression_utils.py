import yaml
import os
from pathlib import Path
from pyspark.sql.functions import col
from utils.spark_session import get_spark_session
from dotenv import load_dotenv

load_dotenv()

base = Path(os.getenv("WORKSPACE_BASE", "./"))
BRONZE_RAW_BASE = base / "bronze_raw"
script_dir = BRONZE_RAW_BASE.parent
CONFIG_PATH = script_dir / "configs" / "config.yaml"

with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)


def get_select_expressions(layer: str, table_name: str):
    rules = CONFIG[layer][table_name]
    select_expressions = []

    for c in rules['final_column_order']:
        alias = c.get("alias")
        if alias:
            select_expressions.append(col(c["name"]).alias(alias))
        else:
            select_expressions.append(col(c["name"]))

    return select_expressions

if __name__ == "__main__":
    spark = get_spark_session()
    a = get_select_expressions("silver", "fct_flights")
    print(*a)