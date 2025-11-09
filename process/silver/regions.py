import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp,
)

from process.io_utils import write_delta_table
from utils.expression_utils import get_select_expressions, get_merge_strategy

from datetime import datetime

def write_openflights_regions_data(
    spark: SparkSession,
    openflights_regions: DataFrame,
    openflights_countries: DataFrame,
    batch_time: datetime
) -> None:
    """
    Combine regions and countries data into dim_regions_df (more regions than countries)
    Then write to delta table
    """
    logging.info("Starting to write openflights regions data.")
    try:
        dim_regions_df = openflights_regions.alias("regions").join(
            openflights_countries.alias("countries").select(
                col("code").alias("country_code"),
                col("name").alias("country_name")
            ),
            col("regions.iso_country") == col("country_code"),
            how="inner"
        )

        dim_regions_df = dim_regions_df.drop("id", "wikipedia_link", col("country_code"), "keywords")

        dim_regions_df = dim_regions_df.withColumn(
            "_ingested_at", lit(batch_time)
        ).withColumn(
            "_inserted_at", current_timestamp()
        )

        select_exprs = get_select_expressions("silver", "dim_regions")
        dim_regions_df = dim_regions_df.select(*select_exprs)

    except:
        logging.error(f"Failed during transformation step for dim_regions_df: {e}", exc_info=True)
        raise

    try:
        logging.info(f"Writing {dim_regions_df.count()} rows to dim_regions_df table.")
        write_delta_table(
            spark=spark, df=dim_regions_df, db_name="silver",
            table_name="dim_regions", write_mode="merge", merge_keys=["region_code"],
        )
        logging.info("Successfully wrote dim_regions_df to Delta table.")

    except Exception as e:
        logging.error(f"Failed during write step for dim_regions_df: {e}", exc_info=True)
        raise