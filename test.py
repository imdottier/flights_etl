from utils.logging_utils import setup_logging
from utils.spark_session import get_spark_session
from pyspark.sql import SparkSession
import logging

from utils.io_utils import read_df
from load.utils import load_table_to_postgres


if __name__ == "__main__":
    spark: SparkSession = get_spark_session()

    logger = setup_logging("test")
    logging.info("Starting the test script...")
    
    silver_airports = read_df(spark, "silver", "dim_airports")
    silver_runways = read_df(spark, "silver", "dim_runways")
    silver_airlines = read_df(spark, "silver", "dim_airlines")
    silver_aircrafts = read_df(spark, "silver", "dim_aircrafts")
    silver_flights = read_df(spark, "silver", "fct_flights")

    from load.utils import load_to_postgres, read_df_from_postgres
    from load.database import transaction_context

    with transaction_context() as master_cursor:
        load_to_postgres(
            spark=spark, df=silver_airports, target_table="dim_airports",
            strategy="upsert", pk_cols=["airport_sk"], cursor=master_cursor
        )
        insert_unknown_airport_sql = f"""
            INSERT INTO dim_airports (airport_sk, airport_iata, airport_icao, airport_name, municipality_name, country_name, continent_name, latitude, longitude, elevation_feet, airport_time_zone)
            VALUES (-1, 'UNK', 'UNK', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 0, 0, 0, 'Unknown')
            ON CONFLICT (airport_sk) DO NOTHING;
        """
        master_cursor.execute(insert_unknown_airport_sql)

        load_to_postgres(
            spark=spark, df=silver_airlines, target_table="dim_airlines",
            strategy="upsert", pk_cols=["airline_sk"], cursor=master_cursor
        )
        load_to_postgres(
            spark=spark, df=silver_aircrafts, target_table="dim_aircrafts",
            strategy="upsert", pk_cols=["aircraft_sk"], cursor=master_cursor
        )

        # unknown_runway = [
        #     {"airport_sk": -1, "runway_name": "UNKNOWN", "true_heading": 0.0, "surface": "Unknown",
        #     "has_lighting": False, "is_closed": False, "length_feet": 0.0, "width_feet": 0.0, "displaced_threshold_feet": 0.0,
        #     "latitude": 0.0, "longitude": 0.0}
        # ]

        load_to_postgres(
            spark=spark, df=silver_runways, target_table="dim_runways",
            strategy="upsert", pk_cols=["runway_sk"], cursor=master_cursor
        )
        insert_unknown_runway_sql = f"""
            INSERT INTO dim_runways (
                runway_sk, airport_sk, runway_name, true_heading, surface, has_lighting,
                is_closed, length_feet, width_feet, displaced_threshold_feet,
                latitude, longitude, effective_start_date, effective_end_date
            )
            VALUES (
                -1, -1, 'UNK', 0.0, 'Unknown', False,
                False, 0.0, 0.0, 0.0,
                0.0, 0.0, '1970-01-01', '9999-12-31'
            )
            ON CONFLICT (runway_sk) DO NOTHING;
        """
        master_cursor.execute(insert_unknown_runway_sql)