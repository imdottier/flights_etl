import os
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

SPARK_WAREHOUSE_DEV = os.getenv("SPARK_WAREHOUSE_DEV", "./spark_warehouse/")

def get_spark_session(app_name: str="Test Pipeline") -> SparkSession:
    mode = os.getenv("SPARK_MODE", "dev")
    builder: SparkSession = SparkSession.builder.appName(app_name)

    logging.info(f"Initializing Spark in {mode} mode.")

    if mode == "dev":
        builder = (
            builder.master("local[*]")
                    .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DEV)
                    .config("spark.hadoop.fs.defaultFS", "file:///")
                    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .enableHiveSupport()
        )

        dev_driver_memory = os.getenv("SPARK_DEV_DRIVER_MEMORY", "4g")
        builder = builder.config("spark.driver.memory", dev_driver_memory)

    return builder.getOrCreate()