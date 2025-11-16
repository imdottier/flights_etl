import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, explode, sha2, concat_ws, lower, trim,
    lit, current_timestamp, coalesce, round as spark_round
)
from functools import reduce

from process.utils import flatten_df
from process.io_utils import merge_delta_table_with_scd
from utils.expression_utils import get_select_expressions, get_merge_strategy
from datetime import datetime


def write_runways_data(
    spark: SparkSession,
    bronze_airports: DataFrame,
    batch_time: datetime
) -> None:
    """
    Process the runways data from the bronze_airports table and write it to the dim_runways_df table.
    
    :param spark: The SparkSession object
    :param bronze_airports: The DataFrame containing the bronze airports data
    :return: None
    """
    
    logging.info("Start writing to dim_runways_df table")
    
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

        # Add the airport_bk column
        flattened_runways_df = flattened_runways_df.withColumn(
            "airport_bk",
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

        flattened_runways_df = (
            flattened_runways_df
            .withColumn(
                "runway_bk",
                sha2(
                    concat_ws(
                        "|",
                        col("airport_bk"),
                        spark_round(col("true_hdg"), 0),
                        spark_round(col("location_lat"), 4),
                        spark_round(col("location_lon"), 4),
                    ),
                    256
                )
            )
            .withColumn("_ingested_at", lit(batch_time))
            .withColumn("_inserted_at", current_timestamp())
        )

        rounded_runways = flattened_runways_df
        merge_strategy = get_merge_strategy("silver", "dim_runways")
        for c, decimals in merge_strategy.items():
            if decimals != -1:
                rounded_runways = rounded_runways.withColumn(c, spark_round(c, decimals))
        
        rounded_runways = rounded_runways.withColumn(
            "location_lat", spark_round(col("location_lat"), 4)
        ).withColumn(
            "location_lon", spark_round(col("location_lon"), 4)
        ).withColumn(
            "true_hdg", spark_round(col("true_hdg"), 1)
        )

        # Get the select expressions for the dim_runways_df table
        select_exprs = get_select_expressions("silver", "dim_runways")
        dim_runways_df = flattened_runways_df.select(*select_exprs)

        business_keys = ["airport_bk", "true_heading", "latitude", "longitude"]
        dim_runways_df = dim_runways_df.filter(
            reduce(lambda a, b: a & b, (col(c).isNotNull() for c in business_keys))
        )

        dim_runways_df = dim_runways_df.dropDuplicates(
            subset=business_keys
        )
        # dim_runways_df = dim_runways_df.unionByName(unknown_runway_df)

    except Exception as e:
        logging.error(f"Failed during transformation step for dim_runways: {e}", exc_info=True)
        raise

    try:
        # Actually always prefer the other source but for demonstration purposes,
        # data changes of a specific col can be tracked in both sources
        logging.info(f"Writing {dim_runways_df.count()} records to dim_runways_df table")
        merge_delta_table_with_scd(
            spark=spark, arriving_df=dim_runways_df, db_name="silver",
            table_name="dim_runways", business_key="runway_bk",
            business_key_version="runway_version_bk", tracked_attribute_cols=merge_strategy,
            business_cols=business_keys,
        )
        logging.info("Successfully wrote dim_runways_df into Delta table")
    
    except Exception as e:
        logging.error(f"Failed to write runways data: {e}", exc_info=True)
        raise e
    

def write_ourairports_runways_data(
    spark: SparkSession,
    ourairports_runways: DataFrame,
    ourairports_airports: DataFrame,
    batch_time: datetime
) -> None:
    logging.info(f"Starting to write ourairports runways data")

    try:
        # Common columns
        common_cols = [
            col("airport_ident"),
            col("length_ft").alias("length_feet"),
            col("width_ft").alias("width_feet"),
            col("surface"),
            col("lighted"),
            col("closed"),
        ]
        # Low-end columns
        low_end_cols = common_cols + [
            col("le_ident").alias("runway_name"),
            col("le_latitude_deg").alias("latitude"),
            col("le_longitude_deg").alias("longitude"),
            col("le_elevation_ft").alias("elevation_feet"),
            col("le_heading_degT").alias("true_heading"),
            col("le_displaced_threshold_ft").alias("displaced_threshold_feet"),
            lit("LE").alias("runway_end_type")  # optional flag
        ]

        # High-end columns
        high_end_cols = common_cols + [
            col("he_ident").alias("runway_name"),
            col("he_latitude_deg").alias("latitude"),
            col("he_longitude_deg").alias("longitude"),
            col("he_elevation_ft").alias("elevation_feet"),
            col("he_heading_degT").alias("true_heading"),
            col("he_displaced_threshold_ft").alias("displaced_threshold_feet"),
            lit("HE").alias("runway_end_type")
        ]

        # Build both DataFrames
        low_end_runways = ourairports_runways.select(*low_end_cols)
        high_end_runways = ourairports_runways.select(*high_end_cols)
        union_runways = low_end_runways.unionByName(high_end_runways)

        filtered_runways = (
            union_runways.join(
                ourairports_airports.select(col("iata_code"), col("icao_code"), col("ident")),
                union_runways.airport_ident == ourairports_airports.ident,
                how="inner"
            ).filter(
                # Business keys can't be null
                col("true_heading").isNotNull() &
                col("latitude").isNotNull() &
                col("longitude").isNotNull()
            )
        )

        # Round columns, including tracked attributes and business keys
        rounded_runways = filtered_runways
        merge_strategy = get_merge_strategy("silver", "dim_runways_ourairports")
        for c, decimals in merge_strategy.items():
            if decimals != -1:
                rounded_runways = rounded_runways.withColumn(c, spark_round(c, decimals))
        
        rounded_runways = rounded_runways.withColumn(
            "latitude", spark_round(col("latitude"), 4)
        ).withColumn(
            "longitude", spark_round(col("longitude"), 4)
        ).withColumn(
            "true_heading", spark_round(col("true_heading"), 0)
        )

        # Standardize columns
        dim_runways_df = rounded_runways.withColumn(
            "has_lighting", col("lighted") == 1
        ).withColumn(
            "is_closed", col("closed") == 1
        ).withColumn(
            "airport_bk",
            sha2(lower(trim(coalesce("iata_code", "icao_code"))), 256)
        ).withColumn(
            "runway_bk",
            sha2(
                concat_ws(
                    "|",
                    col("airport_bk"),
                    spark_round(col("true_heading"), 1),
                    spark_round(col("latitude"), 4),
                    spark_round(col("longitude"), 4),
                ),
                256
            )
        ).withColumn(
            "_ingested_at", lit(batch_time)
        ).withColumn(
            "_inserted_at", current_timestamp()
        )

        dim_runways_df = dim_runways_df.drop(
            "lighted", "closed", "iata_code", "icao_code", "airport_ident", "ident"
        )

        # Apply columns order
        select_exprs = get_select_expressions("silver", "dim_runways_ourairports")
        dim_runways_df = dim_runways_df.select(*select_exprs)

        dim_runways_df = dim_runways_df.dropDuplicates(
            subset=["airport_bk", "true_heading", "latitude", "longitude"]
        )

    except Exception as e:
        logging.error(f"Failed during transformation step for dim_runways: {e}", exc_info=True)
        raise

    try:
        logging.info(f"Writing {dim_runways_df.count()} records to dim_runways_df table")
        merge_delta_table_with_scd(
            spark=spark, arriving_df=dim_runways_df, db_name="silver",
            table_name="dim_runways", business_key="runway_bk",
            business_key_version="runway_version_bk", tracked_attribute_cols=merge_strategy,
            business_cols=["airport_bk", "true_heading", "latitude", "longitude"],
        )
        logging.info("Successfully wrote dim_runways_df into Delta table")
    except Exception as e:
        logging.error(f"Failed to write runways data: {e}", exc_info=True)
        raise e
