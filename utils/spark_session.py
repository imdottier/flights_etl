import os
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import socket

load_dotenv()

SPARK_WAREHOUSE_DEV = os.getenv("SPARK_WAREHOUSE_DEV", "./spark_warehouse/")
SPARK_WAREHOUSE_PROD = os.getenv("SPARK_WAREHOUSE_PROD", "/shared/spark_warehouse/")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://localhost:7077")

SPARK_METASTORE_DB_NAME = os.getenv("SPARK_METASTORE_DB_NAME")
POSTGRES_DB_USER = os.getenv("POSTGRES_DB_USER")
POSTGRES_DB_PASSWORD = os.getenv("POSTGRES_DB_PASSWORD")
POSTGRES_DB_HOST = os.getenv("POSTGRES_DB_HOST")

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_DB_HOST}:5432/{SPARK_METASTORE_DB_NAME}"

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
        print(f" Using warehouse directory: {SPARK_WAREHOUSE_PROD}")
        
        DELTA_JAR_PATH = os.getenv("DELTA_JAR_PATH", "/shared/jars/delta-spark_2.12-3.2.0.jar")
        DELTA_STORAGE_JAR_PATH = os.getenv("DELTA_STORAGE_JAR_PATH", "/shared/jars/delta-storage-3.2.0.jar")
        POSTGRES_JAR_PATH = os.getenv("POSTGRES_JAR_PATH", "/shared/jars/postgresql-42.7.3.jar")

        local_ip = socket.gethostbyname(socket.gethostname())
        print(f"Airflow IP: {local_ip}")
        builder = (
            builder.config("spark.driver.host", local_ip)  # bind inside container
                .config("spark.driver.bindAddress", "0.0.0.0")
                .config("spark.master", SPARK_MASTER_URL)  # e.g., "spark://spark-master:7077"
                .config("spark.executor.cores", "1")
                .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE_PROD)
                .config("spark.jars", f"{DELTA_JAR_PATH},{DELTA_STORAGE_JAR_PATH},{POSTGRES_JAR_PATH}")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.defaultFS", "file:///")
                .config("spark.hadoop.validateOutputSpecs", "false")
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
                .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
                .enableHiveSupport()
        )

    return builder.getOrCreate()