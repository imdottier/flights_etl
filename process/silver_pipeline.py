import logging

from pyspark.sql import SparkSession, DataFrame
from utils.spark_session import get_spark_session
from utils.logging_utils import setup_logging
from utils.io_utils import read_df
from process.utils import enrich_flights_data, csv_to_df

from process.silver.aircrafts import write_aircrafts_data
from process.silver.airlines import write_airlines_data
from process.silver.airports import (
    write_airports_data, write_ourairports_airports_data,
    filter_ourairports_airports
) 
from process.silver.runways import write_runways_data, write_ourairports_runways_data
from process.silver.flights import write_flights_data
from process.silver.regions import write_ourairports_regions_data

from datetime import datetime, timezone, timedelta

import argparse


def run_silver_pipeline(
    spark: SparkSession,
    ingestion_hours: list[str],
    batch_time: datetime,
    process_runways: bool = True,
) -> None:
    """
    Runs the bronze to silver pipeline.

    :param spark: The SparkSession object
    :param include_airports: Whether to write the dim_airports table
    :return: None
    """
    logging.info("="*50)
    logging.info("=== Starting the silver pipeline ===")
    logging.info("="*50)

    # Create the silver database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    enriched_flights_df = None
    try:
        # --- 1. Read Source Data ---
        read_options = {}
        if ingestion_hours:
            logging.info(f"Reading from selected partitions: {ingestion_hours}")
            formatted_list = ", ".join(f"'{p}'" for p in ingestion_hours)
            read_options["where"] = f"ingestion_hour IN ({formatted_list})"
        bronze_flights_df = read_df(spark, "bronze", "flights", **read_options)
        bronze_airports_df = read_df(spark, "bronze", "airports")

        if bronze_flights_df is None:
            logging.error("FATAL: bronze.flights data not found. Cannot proceed.")
            return

        # --- 2. Enrich and CACHE the Central DataFrame ---
        logging.info("Enriching flight data...")
        enriched_flights_df = enrich_flights_data(bronze_flights_df)
        
        enriched_flights_df.cache()
        num_flights = enriched_flights_df.count()
        logging.info(f"Successfully cached {num_flights} enriched flight records.")

        # --- 3. Write Dimensions using the Cached DataFrame ---
        write_aircrafts_data(spark, enriched_flights_df, batch_time)
        write_airlines_data(spark, enriched_flights_df, batch_time)
        write_airports_data(spark, bronze_airports_df, enriched_flights_df, batch_time)

        # Only process runways when there's new data
        if process_runways:
            logging.info("Processing optional runway dimension as requested...")
            if bronze_airports_df is None:
                logging.warning("Cannot process runways because bronze.airports data is missing. Skipping.")
            else:
                write_runways_data(spark, bronze_airports_df, batch_time)
        else:
            logging.info("Skipping optional runway dimension processing as requested.")

        # --- 4. Write the Fact Table using the Cached DataFrame ---
        logging.info("Writing final fact table fct_flights...")
        write_flights_data(spark, enriched_flights_df, batch_time)

    except Exception as e:
        logging.error(f"An unexpected error occurred in the silver pipeline: {e}", exc_info=True)
        raise

    finally:
        # --- 5. Cleanup: Unpersist the DataFrame ---
        if enriched_flights_df:
            logging.info("Unpersisting enriched_flights_df to free up memory.")
            enriched_flights_df.unpersist()
        
        logging.info("="*50)
        logging.info("=== Finished the silver pipeline ===")
        logging.info("="*50)


def run_ourairports_pipeline(
    spark: SparkSession,
) -> None:
    """
    Runs the pipeline that processes the OurAirports data.

    :param spark: The SparkSession object
    :param batch_time: The datetime object representing the batch time
    :return: None
    """
    logging.info("="*50)
    logging.info("=== Starting the silver pipeline ===")
    logging.info("="*50)

    # Create the silver database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    try:
        logging.info("Loading OurAirports data...")
        # Load OurAirports data from CSV files
        ourairports_airports = csv_to_df(spark, "airports.csv")
        ourairports_runways = csv_to_df(spark, "runways.csv")
        ourairports_regions = csv_to_df(spark, "regions.csv")
        ourairports_countries = csv_to_df(spark, "countries.csv")

        # Filter the airports data
        filtered_airports = filter_ourairports_airports(ourairports_airports)

        batch_time = datetime.now(timezone.utc)

        logging.info("Writing OurAirports data...")
        # Write the data to the silver database
        if filtered_airports:
            write_ourairports_airports_data(spark, filtered_airports, batch_time)
        
        if ourairports_runways and filtered_airports:
            write_ourairports_runways_data(
                spark, ourairports_runways,
                filtered_airports, batch_time
            )

        if ourairports_regions and ourairports_countries:
            write_ourairports_regions_data(
                spark, ourairports_regions,
                ourairports_countries, batch_time
            )
    
    except Exception as e:
        logging.error(f"An unexpected error occurred in the silver pipeline: {e}", exc_info=True)
        raise

    finally:
        logging.info("="*50)
        logging.info("=== Finished the silver pipeline ===")
        logging.info("="*50)


# if __name__ == "__main__":
    # log_filepath = setup_logging(log_name="spark_run")
    # spark: SparkSession = get_spark_session()

    # parser = argparse.ArgumentParser(description="Run the Silver pipeline for specific date batches.")
    
    # group = parser.add_mutually_exclusive_group(required=False) # Make it required for testing
    # group.add_argument("--dates", nargs='+', help="A list of YYYY-MM-DD-HH partitions.")

    # group.add_argument(
    #     "-s", "--start-date", 
    #     type=valid_date_format, 
    #     metavar="YYYY-MM-DD-HH", 
    #     help="Start of a date range (inclusive)"
    # )
    # parser.add_argument(
    #     "-e", "--end-date", 
    #     type=valid_date_format,
    #     metavar="YYYY-MM-DD-HH",
    #     help="End of a date range (inclusive). Requires --start-date."
    # )

    # parser.add_argument('--airports', action=argparse.BooleanOptionalAction)

    # args = parser.parse_args()
    # if not args.dates and not args.start_date and not args.end_date:
    #     logging.info("No date arguments provided. Reading all available partitions.")
    #     batches_to_process = None
    # else:
    #     batches_to_process = generate_batch_timestamps(args)
    #     logging.info(f"Generated {len(batches_to_process)} batch(es) to process: {batches_to_process}")


    # run_silver_pipeline(spark, ingestion_hours=batches_to_process, process_runways=args.airports)

    # spark.stop()