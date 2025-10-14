import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from process.utils import write_delta_table

def write_aircrafts_data(
    spark: SparkSession,
    enriched_flights: DataFrame
) -> None:
    """
    Writes the dim_aircrafts table to a Delta table.
    """
    dim_aircrafts = enriched_flights.select(
        "aircraft_sk",
        "aircraft_reg",
        "aircraft_mode_s",
        "aircraft_model",
        "aircraft_sk_type"
    ).dropDuplicates(["aircraft_sk"])

    logging.info("Starting to write aircrafts data")

    try:
        logging.info(f"Writing {dim_aircrafts.count()} rows to dim_aircrafts")
        write_delta_table(
            spark=spark,
            df=dim_aircrafts,
            db_name="silver",
            table_name="dim_aircrafts",
            write_mode="merge",
            merge_keys=["aircraft_sk"]
        )

        logging.info("Successfully wrote dim_aircrafts to Delta table.")

    except Exception as e:
        logging.error(f"Failed to write dim_aircrafts: {e}")
        raise
