import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, coalesce, first, sha2, concat_ws, lit, lower, trim,
    current_timestamp
)
from process.utils import flatten_df
from process.io_utils import merge_delta_table
from utils.expression_utils import get_select_expressions, get_merge_strategy

from datetime import datetime

# Technically we dont need bronze_airports everytime and only 
# on new API data, but for simplicity we will use it every time
def write_airports_data(
    spark: SparkSession,
    bronze_airports: DataFrame,
    enriched_flights: DataFrame,
    batch_time: datetime,
) -> None:
    """
    Writes the dim_airports table to a Delta table.
    """
    logging.info("Start to write airports data")

    try:
        logging.info("Flattening and cleaning detailed airport data.")
        flattened_airports_df = flatten_df(bronze_airports)

        detailed_airports_select_exprs = get_select_expressions("silver", "dim_airports_detailed")
        detailed_airports_df = flattened_airports_df.select(*detailed_airports_select_exprs)

        logging.info("Extracting basic airport list from flight data.")
        departures = enriched_flights.select(
            col("departure_airport_bk").alias("airport_bk"),
            col("departure_airport_iata").alias("airport_iata"),
            col("departure_airport_icao").alias("airport_icao"),
            col("departure_airport_name").alias("airport_name"),
            col("departure_airport_time_zone").alias("airport_time_zone")
        )

        arrivals = enriched_flights.select(
            col("arrival_airport_bk").alias("airport_bk"),
            col("arrival_airport_iata").alias("airport_iata"),
            col("arrival_airport_icao").alias("airport_icao"),
            col("arrival_airport_name").alias("airport_name"),
            col("arrival_airport_time_zone").alias("airport_time_zone")
        )

        all_airports_long_df = departures.unionByName(arrivals)
        all_airports_long_df = all_airports_long_df.filter(col("airport_bk").isNotNull())

        # Get the most detailed info for each airport_bk
        dim_airports_basic_df = (
            all_airports_long_df
            .groupBy("airport_bk")
            .agg(
                first("airport_iata", ignorenulls=True).alias("airport_iata"),
                first("airport_icao", ignorenulls=True).alias("airport_icao"),
                first("airport_name", ignorenulls=True).alias("airport_name"),
                first("airport_time_zone", ignorenulls=True).alias("airport_time_zone")
            )
        )

        logging.info("Enriching basic airport list with detailed data.")
        enriched_df = dim_airports_basic_df.alias("basic").join(
            detailed_airports_df.alias("detailed"),
            on=col("basic.airport_iata") == col("detailed.iata"),
            how="left"
        )

        dim_airports_df = enriched_df.select(
            col("basic.airport_bk"),
            col("basic.airport_iata"),
            coalesce(col("detailed.icao"), col("basic.airport_icao")).alias("airport_icao"),
            coalesce(col("detailed.full_name"), col("basic.airport_name")).alias("airport_name"),
            col("detailed.municipality_name"),
            col("detailed.country_name"),
            col("detailed.latitude"),
            col("detailed.longitude"),
            col("detailed.elevation_feet"),
            coalesce(col("detailed.time_zone"), col("basic.airport_time_zone")).alias("airport_time_zone"),
        )

        # Add hashes and timestamps
        dim_airports_df = (
            dim_airports_df
            .withColumn("_ingested_at", lit(batch_time))
            .withColumn("_inserted_at", current_timestamp())
        )
    
    except Exception as e:
        logging.error(f"Failed during transformation step for dim_airports: {e}", exc_info=True)
        raise

    try:
        merge_strategy = get_merge_strategy("silver", "dim_airports")

        logging.info(f"Writing {dim_airports_df.count()} rows to dim_airports table")
        merge_delta_table(
            spark=spark,
            arriving_df=dim_airports_df,
            db_name="silver",
            table_name="dim_airports",
            merge_keys=["airport_bk"],
            reconciliation_rules=merge_strategy
        )
        logging.info("Successfully wrote dim_airports into Delta table")
    except Exception as e:
        logging.error(f"Failed to write dim_airports into Delta table: {e}", exc_info=True)
        raise e


def filter_openflights_airports(openflights_airports: DataFrame):
    return openflights_airports.filter(
        col("iata_code").isNotNull() | col("icao_code").isNotNull()
    )
    

def write_openflights_airports_data(
    spark: SparkSession,
    openflights_airports: DataFrame,
    batch_time: datetime
):
    """Write openflights airports data to a Delta table."""
    logging.info(f"Starting to write openflights airports data.")

    try:
        logging.info("Cleaning detailed airport data.")
        dim_airports_openflights_df = openflights_airports.withColumn(
            "airport_bk",
            sha2(lower(trim(coalesce("iata_code", "icao_code"))), 256)
        ).withColumn(
            "scheduled_service", col("scheduled_service") == "yes"
        ).withColumn(
            "_ingested_at", lit(batch_time)
        ).withColumn(
            "_inserted_at", current_timestamp()
        )

        dim_airports_openflights_df = dim_airports_openflights_df.drop(
            "id", "ident", "continent",
            "home_link", "wikipedia_link", "keywords"
        )

        select_exprs = get_select_expressions("silver", "dim_airports_openflights")
        dim_airports_openflights_df = dim_airports_openflights_df.select(*select_exprs)

    except Exception as e:
        logging.error(f"Failed during transformation step for dim_airports_openflights: {e}", exc_info=True)
        raise

    try:
        merge_strategy = get_merge_strategy("silver", "dim_airports_openflights")

        logging.info(f"Writing {dim_airports_openflights_df.count()} rows to dim_airports_openflights table")
        merge_delta_table(
            spark=spark,
            arriving_df=dim_airports_openflights_df,
            db_name="silver",
            table_name="dim_airports",
            merge_keys=["airport_bk"],
            reconciliation_rules=merge_strategy
        )
        logging.info("Successfully wrote dim_airports_openflights into Delta table")
    
    except Exception as e:
        logging.error(f"Failed to write dim_airports_openflights into Delta table: {e}", exc_info=True)
        raise