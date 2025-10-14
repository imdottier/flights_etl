import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, coalesce

from process.utils import write_delta_table, flatten_df
from utils.expression_utils import get_select_expressions


import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, coalesce

from process.utils import write_delta_table, flatten_df
from utils.expression_utils import get_select_expressions

# Technically we dont need bronze_airports everytime and only 
# on new API data, but for simplicity we will use it every time
def write_airports_data(
    spark_session: SparkSession,
    bronze_airports: DataFrame,
    enriched_flights: DataFrame
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
        arrival_airports_df = (
            enriched_flights.select(
                col("arrival_airport_sk").alias("airport_sk"),
                col("arrival_airport_iata").alias("airport_iata"),
                col("arrival_airport_icao").alias("airport_icao"),
                col("arrival_airport_name").alias("airport_name"),
                col("arrival_airport_time_zone").alias("airport_time_zone"),
            )
        )

        departure_airports_df = (
            enriched_flights.select(
                col("departure_airport_sk").alias("airport_sk"),
                col("departure_airport_iata").alias("airport_iata"),
                col("departure_airport_icao").alias("airport_icao"),
                col("departure_airport_name").alias("airport_name"),
                col("departure_airport_time_zone").alias("airport_time_zone"),
            )
        )

        dim_airports_basic_df = departure_airports_df.unionByName(arrival_airports_df).drop_duplicates(["airport_sk"])

        logging.info("Enriching basic airport list with detailed data.")
        enriched_df = dim_airports_basic_df.alias("basic").join(
            detailed_airports_df.alias("detailed"),
            on=col("basic.airport_iata") == col("detailed.iata"),
            how="left"
        )

        dim_airports_df = enriched_df.select(
            col("basic.airport_sk"),
            col("basic.airport_iata"),
            coalesce(col("detailed.icao"), col("basic.airport_icao")).alias("airport_icao"),
            coalesce(col("detailed.full_name"), col("basic.airport_name")).alias("airport_name"),
            col("detailed.municipality_name"),
            col("detailed.country_name"),
            col("detailed.continent_name"),
            col("detailed.latitude"),
            col("detailed.longitude"),
            col("detailed.elevation_feet"),
            coalesce(col("detailed.time_zone"), col("basic.airport_time_zone")).alias("airport_time_zone"),
        )
    
    except Exception as e:
        logging.error(f"Failed during transformation step for dim_airports: {e}", exc_info=True)
        raise

    try:
        logging.info(f"Writing {dim_airports_df.count()} rows to dim_airports table")
        write_delta_table(
            spark=spark_session,
            df=dim_airports_df,
            db_name="silver",
            table_name="dim_airports",
            write_mode="merge",
            merge_keys=["airport_sk"]
        )
        logging.info("Successfully wrote dim_airports into Delta table")
    except Exception as e:
        logging.error(f"Failed to write dim_airports into Delta table: {e}", exc_info=True)
        raise e