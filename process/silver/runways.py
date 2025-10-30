import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, explode, sha2, concat_ws, lower, trim,
    lit, current_timestamp
)

from process.utils import write_delta_table_with_scd, flatten_df
from utils.expression_utils import get_select_expressions
from datetime import datetime


def write_runways_data(
    spark: SparkSession,
    bronze_airports: DataFrame,
    batch_time: datetime
) -> None:
    """
    Process the runways data from the bronze_airports table and write it to the dim_runways table.
    
    :param spark: The SparkSession object
    :param bronze_airports: The DataFrame containing the bronze airports data
    :return: None
    """
    
    logging.info("Start writing to dim_runways table")
    
    try:
        # Flatten the runways data
        flattened_airports_df = flatten_df(bronze_airports)

        # Explode the runways data
        exploded_runways = (
            flattened_airports_df.select(
                col("iata").alias("airport_pk"),
                explode("runways").alias("data")
            )
        )

        # Flatten the exploded runways data
        flattened_runways_df = flatten_df(
            exploded_runways.select(
                "airport_pk",
                "data.*"
            )
        )

        # Add the airport_sk column
        flattened_runways_df = flattened_runways_df.withColumn(
            "airport_sk",
            sha2(lower(trim(col("airport_pk"))), 256)
        )

        # # Define the unknown runway data
        # unknown_runway = [
        #     {"airport_sk": -1, "runway_name": "UNKNOWN", "true_heading": 0.0, "surface": "Unknown",
        #     "has_lighting": False, "is_closed": False, "length_feet": 0.0, "width_feet": 0.0, "displaced_threshold_feet": 0.0,
        #     "latitude": 0.0, "longitude": 0.0}
        # ]

        # # Create the unknown runway DataFrame
        # unknown_runway_df = spark.createDataFrame(unknown_runway)

        data_cols = [
            c for c in flattened_runways_df.columns
            if c not in ["airport_sk", "true_heading", "latitude", "longitude"]
        ]
        flattened_runways_df = (
            flattened_runways_df
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

        # Get the select expressions for the dim_runways table
        select_exprs = get_select_expressions("silver", "dim_runways")
        dim_runways = flattened_runways_df.select(*select_exprs)
        # dim_runways = dim_runways.unionByName(unknown_runway_df)

    except Exception as e:
        logging.error(f"Failed during transformation step for dim_runways: {e}", exc_info=True)
        raise

    try:    
        logging.info(f"Writing {dim_runways.count()} records to dim_runways table")
        write_delta_table_with_scd(
            spark=spark,
            df=dim_runways,
            db_name="silver",
            table_name="dim_runways",
            business_keys=["airport_sk", "true_heading", "latitude", "longitude"],
            surrogate_key="runway_sk",
            surrogate_key_version="runway_version_key"
        )
        logging.info("Successfully wrote dim_runways into Delta table")
    except Exception as e:
        logging.error(f"Failed to write runways data: {e}", exc_info=True)
        raise e
