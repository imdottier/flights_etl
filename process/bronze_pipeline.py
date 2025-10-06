from pyspark.sql.types import StructType
from pyspark.sql import SparkSession
from configs.schemas import flights_schema, full_airport_schema

from utils.spark_session import get_spark_session
from utils.logging_utils import setup_logging
from process.utils import json_to_df, write_delta_table


if __name__ == "__main__":
    log_filepath = setup_logging(log_name="spark_run")
    spark: SparkSession = get_spark_session()

    flights_df = json_to_df(
        spark=spark,
        table_name="flights", 
        schema=flights_schema
    )

    airports_df = json_to_df(
        spark=spark,
        table_name="airports", 
        schema=full_airport_schema
    )

    spark.sql("DROP DATABASE IF EXISTS bronze CASCADE")

    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

    write_delta_table(
        spark=spark,
        df=flights_df,
        db_name="bronze",
        table_name="flights",
        partition_cols=["ingestion_hour"]
    )

    write_delta_table(
        spark=spark,
        df=airports_df,
        db_name="bronze",
        table_name="airports",
    )