import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp, col, sha2, concat_ws, lit, lower, trim,
)
from datetime import datetime

from process.utils import write_delta_table

def write_airlines_data(
    spark: SparkSession,
    enriched_flights: DataFrame,
    batch_time: datetime,
) -> None:
    """
    Writes the dim_airlines table to a Delta table.
    """
    dim_airlines = enriched_flights.filter(
        col("airline_sk").isNotNull()
    ).select(
        col("airline_sk").alias("airline_sk"),
        col("airline_iata").alias("airline_iata"),
        col("airline_icao").alias("airline_icao"),
        col("airline_name").alias("airline_name"),
    ).dropDuplicates(["airline_sk"])

    data_cols = [c for c in dim_airlines.columns if c != "airline_sk"]

    dim_airlines = (
        dim_airlines
        .withColumn(
            "_data_hash",
            sha2(
                concat_ws(
                    "|",
                    *[lower(trim(col(c))) for c in data_cols]
                ),
            256)
        )
        .withColumn("_ingested_at", lit(batch_time))
        .withColumn("_inserted_at", current_timestamp())
    )

    logging.info("Starting to write airlines data")

    try:
        logging.info(f"Writing {dim_airlines.count()} rows to dim_airlines")
        write_delta_table(
            spark=spark,
            df=dim_airlines,
            db_name="silver",
            table_name="dim_airlines",
            write_mode="merge",
            merge_keys=["airline_sk"]
        )

        logging.info("Successfully wrote dim_airlines to Delta table.")

    except Exception as e:
        logging.error(f"Failed to write dim_airlines: {e}")
        raise
