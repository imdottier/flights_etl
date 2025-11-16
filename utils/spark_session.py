import os
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

SPARK_WAREHOUSE_DEV = os.getenv("SPARK_WAREHOUSE_DEV", "./spark_warehouse/")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_DB_USER = os.getenv("POSTGRES_DB_USER")
POSTGRES_DB_PASSWORD = os.getenv("POSTGRES_DB_PASSWORD")
POSTGRES_DB_HOST = os.getenv("POSTGRES_DB_HOST")

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_DB_HOST}:5432/{POSTGRES_DB_NAME}"

def get_spark_session(app_name: str="Test Pipeline") -> SparkSession:
    mode = os.getenv("SPARK_MODE", "dev")
    builder: SparkSession = SparkSession.builder.appName(app_name)

    logging.info(f"Initializing Spark in {mode} mode.")

    if mode == "dev":
        builder = (
            builder.master("local[*]")
                    .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DEV)
                    .config("spark.hadoop.fs.defaultFS", "file:///")
                    .config("spark.hadoop.validateOutputSpecs", "false")
                    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                    .config("spark.driver.memory", "4g")
                    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.ui.showConsoleProgress", "false")
                    .config("spark.sql.debug.maxToStringFields", "10")
                    .config("spark.executor.extraJavaOptions", "-Dlog4j2.formatMsgNoLookups=true")
                    .config("spark.driver.extraJavaOptions",
                           "-Dlog4j2.formatMsgNoLookups=true "
                           "-Dorg.apache.spark.ui.showConsoleProgress=false "
                           "-Dlog4j.rootCategory=ERROR,console "
                           "-Dlog4j.logger.org.apache.spark=ERROR "
                           "-Dlog4j.logger.org.spark_project=ERROR "
                           "-Dlog4j.logger.org.apache.hadoop=ERROR "
                           "-Dlog4j.logger.io.delta=ERROR")
                    .enableHiveSupport()
        )

        dev_driver_memory = os.getenv("SPARK_DEV_DRIVER_MEMORY", "4g")
        builder = builder.config("spark.driver.memory", dev_driver_memory)

    elif mode == "prod":
        builder = (
            builder.config("spark.driver.host", SPARK_MASTER)
                .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_DEV) # Still local for now
                .config("spark.hadoop.fs.defaultFS", "file:///")
                .config("spark.hadoop.validateOutputSpecs", "false")
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                # Use Postgres for Hive metastore
                .config("javax.jdo.option.ConnectionURL", POSTGRES_JDBC_URL)
                .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
                .config("javax.jdo.option.ConnectionUserName", POSTGRES_DB_USER)
                .config("javax.jdo.option.ConnectionPassword", POSTGRES_DB_PASSWORD)
                .enableHiveSupport()
        )

    return builder.getOrCreate()