import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, xxhash64

from process.utils import write_delta_table_with_scd, flatten_df
from utils.expression_utils import get_select_expressions

def write_runways_data(
    spark: SparkSession,
    bronze_airports: DataFrame
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
            xxhash64(col("airport_pk"))
        )

        # # Define the unknown runway data
        # unknown_runway = [
        #     {"airport_sk": -1, "runway_name": "UNKNOWN", "true_heading": 0.0, "surface": "Unknown",
        #     "has_lighting": False, "is_closed": False, "length_feet": 0.0, "width_feet": 0.0, "displaced_threshold_feet": 0.0,
        #     "latitude": 0.0, "longitude": 0.0}
        # ]

        # # Create the unknown runway DataFrame
        # unknown_runway_df = spark.createDataFrame(unknown_runway)

        # Get the select expressions for the dim_runways table
        r_select_exprs = get_select_expressions("silver", "dim_runways")
        dim_runways = flattened_runways_df.select(*r_select_exprs)
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
            surrogate_key="runway_sk"
        )
        logging.info("Successfully wrote dim_runways into Delta table")
    except Exception as e:
        logging.error(f"Failed to write runways data: {e}", exc_info=True)
        raise e
