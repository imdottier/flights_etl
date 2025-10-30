import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, coalesce, lit, current_timestamp

from utils.io_utils import read_df
from process.utils import write_delta_table
from utils.expression_utils import get_select_expressions

from datetime import datetime

def write_flights_data(
    spark: SparkSession,
    enriched_flights: DataFrame,
    batch_time: datetime,
) -> None:
    """
    Writes the fct_flights table to a Delta table.
    
    Perform dimension lookup on dim_runways table.
    """
    logging.info("Starting to write flights data")
    
    try:
        logging.info("Reading dim_runways table")
        silver_runways = read_df(spark, "silver", "dim_runways")

        if silver_runways is None:
            logging.error("FATAL: dim_runways data not found. Cannot proceed.")
            return
        
        effective_runways = silver_runways.filter(col("effective_end_date").isNull())
        if effective_runways.count() == 0:
            logging.error("FATAL: dim_runways data not found. Cannot proceed.")
            return
        
        # Perform dimension lookup on dim_runways table
        dep_runway_dim = effective_runways.alias("dep")
        arr_runway_dim = effective_runways.alias("arr")

        # Join the flights table with the runways table
        fct_flights = (
            enriched_flights
            .join(
                dep_runway_dim,
                on=[enriched_flights["departure_runway"] == dep_runway_dim["runway_name"], enriched_flights["departure_airport_sk"] == dep_runway_dim["airport_sk"]],
                how="left"
            )
            .join(
                arr_runway_dim,
                on=[enriched_flights["arrival_runway"] == arr_runway_dim["runway_name"], enriched_flights["arrival_airport_sk"] == arr_runway_dim["airport_sk"]],
                how="left"
            )
            .drop(dep_runway_dim["_ingested_at"])
            .drop(dep_runway_dim["_inserted_at"])
            .drop(arr_runway_dim["_ingested_at"])
            .drop(arr_runway_dim["_inserted_at"])
        )

        # # Get the unknown runway sk
        # unknown_runway_sk = silver_runways.filter(col("airport_sk") == -1).select("runway_sk").collect()[0]["runway_sk"]

        # Replace the null runway sk with the unknown runway sk
        fct_flights = fct_flights.withColumn(
            "departure_runway_version_key",
            col("dep.runway_version_key")
            # coalesce(col("dep.runway_sk"), lit(unknown_runway_sk))
        ).withColumn(
            "arrival_runway_version_key",
            col("arr.runway_version_key")
            # coalesce(col("arr.runway_sk"), lit(unknown_runway_sk))
        )

        # # Fill the null values of the runway sk with the unknown runway sk
        # fct_flights = fct_flights.fillna(unknown_runway_sk, subset=["departure_runway_sk", "arrival_runway_sk"])

        # Add timestamps
        fct_flights = (
            fct_flights
            .withColumn("_ingested_at", lit(batch_time))
            .withColumn("_inserted_at", current_timestamp())
        )

        # Get the select expressions for the fct_flights table
        select_exprs = get_select_expressions("silver", "fct_flights")
        fct_flights = fct_flights.select(*select_exprs)

    except Exception as e:
        logging.error(f"Failed during transformation step for fct_flights: {e}", exc_info=True)
        raise

    try:
        logging.info(f"Writing {fct_flights.count()} rows to fct_flights table")
        write_delta_table(
            spark=spark,
            df=fct_flights,
            db_name="silver",
            table_name="fct_flights",
            write_mode="overwrite_partitions",
            partition_cols=["ingestion_hour"]
        )
        logging.info("Successfully wrote fct_flights into Delta table")
    except Exception as e:
        logging.error(f"Failed to write flights data: {e}", exc_info=True)
        raise

