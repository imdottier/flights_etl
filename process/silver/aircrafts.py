import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp, col, sha2, concat_ws, lit, lower, trim, coalesce
)
from datetime import datetime

from process.io_utils import write_delta_table

def write_aircrafts_data(
    spark: SparkSession,
    enriched_flights: DataFrame,
    batch_time: datetime,
) -> None:
    """
    Writes the dim_aircrafts table to a Delta table.
    """
    dim_aircrafts = enriched_flights.filter(
        col("aircraft_bk").isNotNull()
    ).select(
        "aircraft_bk",
        "aircraft_reg",
        "aircraft_mode_s",
        "aircraft_model",
        "aircraft_bk_type"
    ).dropDuplicates(["aircraft_bk"])

    # Add hashes and timestamps
    data_cols = [c for c in dim_aircrafts.columns if c != "aircraft_bk"]
    dim_aircrafts = (
        dim_aircrafts
        .withColumn("_ingested_at", lit(batch_time))
        .withColumn("_inserted_at", current_timestamp())
    )

    logging.info("Starting to write aircrafts data")

    try:
        logging.info(f"Writing {dim_aircrafts.count()} rows to dim_aircrafts")
        write_delta_table(
            spark=spark,
            df=dim_aircrafts,
            db_name="silver",
            table_name="dim_aircrafts",
            write_mode="merge",
            merge_keys=["aircraft_bk"]
        )

        logging.info("Successfully wrote dim_aircrafts to Delta table.")

    except Exception as e:
        logging.error(f"Failed to write dim_aircrafts: {e}")
        raise
