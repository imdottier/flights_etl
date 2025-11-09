import os
import logging
from pathlib import Path

import re
import pandas as pd

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    pandas_udf, col, coalesce, to_timestamp, explode,
    sha2, concat_ws, lit,
    lower, trim, when, length, regexp_extract,
)

from pyspark.sql.types import StructType,  StringType
from delta.tables import DeltaTable

from dotenv import load_dotenv

load_dotenv()


base = Path(os.getenv("WORKSPACE_BASE", "./"))
BRONZE_RAW_BASE = base / "bronze_raw"

def json_to_df(
    spark: SparkSession,
    table_name: str,
    schema: StructType,
    ingestion_hours: list[str] | None = None,
):
    """
    Reads JSON files from a given path and enforce a default schema to it

    Args:
        spark: The active SparkSession.
        table_name: The name of the table to read from.
        schema: The schema to enforce on the read DataFrame.
        ingestion_hours: The list of ingestion hours to read from. If None, all partitions will be read.

    Returns:
        DataFrame: The DataFrame read from the given path.
    """
    base_path = BRONZE_RAW_BASE / table_name

    if ingestion_hours:
        # Read selected hours
        paths = [str(base_path / f"ingestion_hour={hour}") for hour in ingestion_hours]
        logging.info(f"Reading from selected partitions: {paths}")
    else:
        # Read all partitions
        logging.info(f"Reading from all partitions in: {base_path}")
        paths = str(base_path)
    
    try:
        logging.info(f"Trying to read JSON from path '{paths}'")

        reader = spark.read.schema(schema).option("basePath", str(base_path))
        df = reader.json(paths)

        logging.info(f"Successfully read data into a DataFrame.")
        return df

    except Exception as e:
        logging.error(f"Failed to read JSON from path '{paths}'. Error: {e}", exc_info=True)
        raise


def csv_to_df(spark: SparkSession, file_name: str) -> DataFrame:
    file_path = base / "csv" / file_name
    return spark.read.csv(str(file_path), header=True, inferSchema=True)


def camel_to_snake(name: str) -> str:
    """Converts a single string from camelCase to snake_case."""
    if name is None:
        return None
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


def flatten_df(df: DataFrame, cols_to_snake_case_values: set[str] | None = None) -> DataFrame:
    if cols_to_snake_case_values is None:
        cols_to_snake_case_values = set()

    @pandas_udf(StringType())
    def camel_to_snake_vectorized(series: pd.Series) -> pd.Series:
        return series.str.replace(r'(?<!^)(?=[A-Z])', '_', regex=True).str.lower()
    
    flat_cols = []

    def get_flat_cols(schema: StructType, prefix: str = ""):
        for field in schema.fields:
            full_col_name = f"{prefix}.{field.name}" if prefix else field.name

            if isinstance(field.dataType, StructType):
                get_flat_cols(field.dataType, prefix=f"{full_col_name}")
            else:
                alias_name = camel_to_snake(full_col_name.replace(".", "_"))

                if alias_name in cols_to_snake_case_values and isinstance(field.dataType, StringType):
                    flat_cols.append(
                        camel_to_snake_vectorized(col(full_col_name)).alias(alias_name)
                    )
                else:
                    flat_cols.append(
                        col(full_col_name).alias(alias_name)
                    )

    try:
        get_flat_cols(df.schema)
        
        if not flat_cols:
            logging.warning("flatten_df resulted in no columns. Returning an empty DataFrame.")
            return df.sparkSession.createDataFrame([], schema=StructType([]))
            
        return df.select(flat_cols)

    except Exception as e:
        logging.error("An error occurred during DataFrame flattening.", exc_info=True)
        raise


def transform_timestamps(df: DataFrame) -> DataFrame:
    source_format = "yyyy-MM-dd HH:mm'Z'"
    iso_8601_format = "yyyy-MM-dd'T'HH:mm:ss'Z'"
    local_with_offset_format = "yyyy-MM-dd HH:mmXXX"

    utc_cols = [c for c in df.columns if c.endswith("_utc")]
    local_cols = [c for c in df.columns if c.endswith("_local")]

    for utc_col in utc_cols:
        df = df.withColumn(
            utc_col,
            coalesce(
                to_timestamp(col(utc_col), source_format),
                to_timestamp(col(utc_col), iso_8601_format),
            )
        )

    for local_col in local_cols:
        df = df.withColumn(
            local_col + "_ntz",
            to_timestamp(col(local_col), local_with_offset_format)
        )

    return df


def enrich_flights_data(bronze_flights: DataFrame) -> DataFrame:
    """
    Enrich the bronze flights data by adding BKs (airport, flight, airline, aircraft),
    best scheduled time UTC, composite PK, and time zone info.

    Args:
        bronze_flights (DataFrame): The bronze flights data.

    Returns:
        DataFrame: Enriched flights DataFrame.
    """

    # --- Helper: build airport BK ---
    def airport_bk(iata, icao, name):
        return sha2(lower(trim(coalesce(iata, icao, name))), 256)

    # --- Helper: flatten and timestamp-transform ---
    def preprocess(df, field):
        exploded = df.select(explode(field).alias("data"), "airport", "ingestion_hour")
        flat = flatten_df(exploded.select("data.*", "airport", "ingestion_hour"))
        return transform_timestamps(flat)

    # --- Departure flights ---
    dep_df = preprocess(bronze_flights, "departures").withColumns({
        "departure_airport_bk": sha2(lower(trim(col("airport"))), 256),
        "arrival_airport_bk": airport_bk(
            col("arrival_airport_iata"),
            col("arrival_airport_icao"),
            col("arrival_airport_name"),
        ),
    })

    # --- Arrival flights ---
    arr_df = preprocess(bronze_flights, "arrivals").withColumns({
        "departure_airport_bk": airport_bk(
            col("departure_airport_iata"),
            col("departure_airport_icao"),
            col("departure_airport_name"),
        ),
        "arrival_airport_bk": sha2(lower(trim(col("airport"))), 256),
    })

    # --- Common fields for both ---
    def enrich_common(df):
        return (
            df.withColumn(
                "best_scheduled_time_utc",
                coalesce(col("departure_scheduled_time_utc"), col("arrival_scheduled_time_utc")),
            )
            .withColumn(
                "flight_composite_pk",
                concat_ws(
                    "|",
                    col("best_scheduled_time_utc"),
                    col("number"),
                    col("departure_airport_bk"),
                    col("arrival_airport_bk"),
                ),
            )
            .withColumn("flight_sk", sha2(col("flight_composite_pk"), 256))
        )

    dep_df = enrich_common(dep_df)
    arr_df = enrich_common(arr_df)

    # --- Merge and deduplicate ---
    flights = dep_df.union(arr_df).dropDuplicates(["flight_sk"])

    # --- Airline BK ---
    airline_code = when(
        (length(col("airline_name")).between(3, 4)) & col("airline_name").rlike("^[A-Z]+$"),
        col("airline_name"),
    ).otherwise(lower(trim(col("airline_name"))))

    flights = flights.withColumn(
        "airline_bk",
        sha2(lower(trim(coalesce(col("airline_iata"), col("airline_icao"), airline_code))), 256),
    )

    # --- Aircraft BK + type ---
    flights = flights.withColumns({
        "aircraft_bk": sha2(
            lower(trim(
                when(
                    col("aircraft_reg").isNull()
                    & col("aircraft_mode_s").isNull()
                    & col("aircraft_model").isNull(),
                    lit("unknown")
                ).otherwise(
                    coalesce(col("aircraft_reg"), col("aircraft_mode_s"), col("aircraft_model"))
                )
            )),
            256
        ),
        "aircraft_bk_type": when(col("aircraft_reg").isNotNull(), lit("reg"))
        .when(col("aircraft_mode_s").isNotNull(), lit("mode_s"))
        .when(col("aircraft_model").isNotNull(), lit("model"))
        .otherwise(lit("unknown")),
    })

    # --- Time zone info ---
    flights = flights.withColumns({
        "dep_local_timezone": regexp_extract(col("departure_scheduled_time_local"), r"([+-]\d{2}:\d{2}|Z)$", 1),
        "arr_local_timezone": regexp_extract(col("arrival_scheduled_time_local"), r"([+-]\d{2}:\d{2}|Z)$", 1),
    })

    return flights