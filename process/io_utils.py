import os
import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, coalesce, sha2, concat_ws, current_timestamp, lit,
    lower, trim, round as spark_round
)
from delta.tables import DeltaTable

from dotenv import load_dotenv

load_dotenv()


base = Path(os.getenv("WORKSPACE_BASE", "./"))
BRONZE_RAW_BASE = base / "bronze_raw"


def write_delta_table(
    spark: SparkSession,
    df: DataFrame,
    db_name: str,
    table_name: str,
    write_mode: str, # 'overwrite_table', 'overwrite_partitions', 'merge'
    merge_keys: list[str] | None = None,
    partition_cols: list[str] | None = None,
):
    """
    Writes a DataFrame to a Delta table using dynamic partition overwrite.
    Creates the table with specified partitioning if it does not exist.

    Args:
        spark: The active SparkSession.
        df: The DataFrame to write.
        db_name: The database name (e.g., 'bronze', 'silver').
        table_name: The name of the target table.
        partition_cols: A list of column names to partition the table by (only used on creation).
    """
    full_table_name = f"{db_name}.{table_name}"
    table_path = f"{base}/{db_name}/{table_name}"
    logging.info(f"--- Preparing to write to Delta table: {full_table_name} ---")

    # Safety checks
    if write_mode == "overwrite_partitions" and not partition_cols:
        raise ValueError("FATAL: 'partition_cols' must be provided for 'overwrite_partitions' mode.")
    if write_mode == "merge" and not merge_keys:
        raise ValueError("FATAL: 'merge_keys' must be provided for 'merge' mode.")

    try:
        table_exists = spark.catalog.tableExists(full_table_name)

        if not table_exists:
            logging.info(f"Table '{full_table_name}' does not exist. Creating new external table...")
            
            if partition_cols and write_mode in ["overwrite_table", "overwrite_partitions"]:
                logging.info(f"Creating and partitioning table by: {partition_cols}")
                (
                    df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .partitionBy(*partition_cols)
                    .save(table_path)
                )

            # Case 2: The table will NOT be partitioned
            else:
                logging.info("Creating a new unpartitioned table.")
                (
                    df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true") # Safe to use here
                    .save(table_path)
                )

            spark.sql(f"CREATE TABLE {full_table_name} USING DELTA LOCATION '{table_path}'")
            logging.info(f"✅ Table '{full_table_name}' successfully created.")
            # Since the table was just created with the full data, the job is done.
            return

        # --- Logic for Full Table Overwrite ---
        if write_mode == "overwrite_table":
            logging.info(f"Performing a FULL table overwrite for {full_table_name}.")
            writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            
            # Partitioning is only applied on creation
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            writer.save(table_path)
            # Ensure table is registered in metastore after the data is written
            spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table_name} USING DELTA LOCATION '{table_path}'")

        # --- Logic for Dynamic Partition Overwrite ---
        elif write_mode == "overwrite_partitions":
            logging.info(f"Performing a DYNAMIC PARTITION overwrite for {full_table_name}.")
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            df.write.format("delta").mode("overwrite").option("mergeSchema", "true") \
                .partitionBy(*partition_cols).save(table_path)
    
        # --- Logic for Merge ---
        # Used only if target table expects data from 1 source
        # use merge_delta_table otherwise
        elif write_mode == "merge":
            logging.info(f"Performing a MERGE operation for {full_table_name}.")
            delta_table = DeltaTable.forPath(spark, table_path)
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            
            (
                delta_table.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            
        else:
            raise ValueError(f"Invalid write_mode '{write_mode}'. Must be 'overwrite_table', 'overwrite_partitions', or 'merge'.")

        logging.info(f"✅ Write operation '{write_mode}' for {full_table_name} complete.")

    except Exception as e:
        logging.error(f"FATAL: Failed during WRITE phase for '{full_table_name}'.", exc_info=True)
        raise


def align_schema(
    spark: SparkSession,
    source_df: DataFrame,
    target_table_name: str,
    target_table_path: str,
):
    target = DeltaTable.forPath(spark, target_table_path)
    target_df = target.toDF()

    target_columns = {f.name for f in target_df.schema.fields}
    source_schema = {f.name: f.dataType.simpleString() for f in source_df.schema.fields}

    missing_columns = source_schema.keys() - target_columns
    if not missing_columns:
        return

    logging.warning(f"Schema mismatch detected. Adding missing columns: {missing_columns}")

    # Build ALTER TABLE query using correct data types
    alter_clauses = []
    for col_name in missing_columns:
        data_type = source_schema[col_name]
        alter_clauses.append(f"`{col_name}` {data_type.upper()}")

    alter_query = f"""
        ALTER TABLE {target_table_name}
        ADD COLUMNS ({', '.join(alter_clauses)})
    """
    spark.sql(alter_query)


def merge_delta_table(
    spark: SparkSession,
    arriving_df: DataFrame,
    db_name: str,
    table_name: str,
    merge_keys: list[str],
    reconciliation_rules: dict[str, str],
    metadata_cols: list[str] = ["_ingested_at", "_inserted_at"],
):
    """
    Reconciles an arriving DataFrame against the current state of the silver Delta table
    and merges the resulting "golden record" back.
    """
    full_table_name = f"{db_name}.{table_name}"
    table_path = f"{base}/{db_name}/{table_name}"

    try:
        if not spark.catalog.tableExists(full_table_name):
            logging.info(f"Table '{full_table_name}' does not exist. Creating a new table...")
            data_cols = [c for c in arriving_df.columns if c not in merge_keys + metadata_cols]
            arriving_df = arriving_df.withColumn(
                "_data_hash", 
                sha2(
                    concat_ws(
                        "|",
                        *[coalesce(lower(trim(col(c))), lit("NULL")) for c in data_cols]
                    ),
                256)
            )

            arriving_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
            spark.sql(f"CREATE TABLE {full_table_name} USING DELTA LOCATION '{table_path}'")
            logging.info(f"Table '{full_table_name}' created successfully. Merge operation skipped as table was just created.")
            return
    
        logging.info(f"--- Preparing to MERGE into Delta table: {full_table_name} ---")

        align_schema(spark, arriving_df, full_table_name, table_path)

        target_table = DeltaTable.forPath(spark, table_path)
        existing_df = target_table.toDF().alias("existing")
        arriving_df = arriving_df.alias("arriving")

        # Construct the join condition
        join_condition = [existing_df[key] == arriving_df[key] for key in merge_keys]
        # Perform the full outer join
        compilation_df = arriving_df.join(
            existing_df, join_condition, "left"
        )

        # Apply the merge strategies
        all_cols = list(dict.fromkeys(existing_df.columns + arriving_df.columns))

        select_exprs = []
        for column_name in all_cols:
            if column_name == "_data_hash":
                continue
            elif column_name in merge_keys:
                expr = coalesce(col(f"existing.{column_name}"), col(f"arriving.{column_name}")).alias(column_name)
            elif column_name in reconciliation_rules:
                strategy = reconciliation_rules[column_name]
                if strategy == "prefer_arriving":
                    expr = coalesce(col(f"arriving.{column_name}"), col(f"existing.{column_name}")).alias(column_name)
            elif column_name in metadata_cols:
                # If it's a match then always new metadata
                expr = col(f"arriving.{column_name}").alias(column_name)
            else:
                if column_name in arriving_df.columns:
                    # Default behavior for columns not in rules: prefer existing
                    expr = coalesce(col(f"existing.{column_name}"), col(f"arriving.{column_name}")).alias(column_name)
                else:
                    expr = col(f"existing.{column_name}").alias(column_name)

            select_exprs.append(expr)

        # Select the merged columns
        golden_records_df = compilation_df.select(*select_exprs)

        # Prepare for merge
        merge_condition_str = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])


        data_cols = [c for c in golden_records_df.columns if c not in merge_keys + metadata_cols]
        golden_records_with_hash = golden_records_df.withColumn(
            "_data_hash", 
            sha2(
                concat_ws(
                    "|",
                    *[coalesce(lower(trim(col(c))), lit("NULL")) for c in data_cols]
                ),
            256)
        )

        (
            target_table.alias("target")
            .merge(
                golden_records_with_hash.alias("source"),
                merge_condition_str
            )
            .whenMatchedUpdateAll(condition="target._data_hash <> source._data_hash") # Use the hash here
            .whenNotMatchedInsertAll()
            .execute()
        )
        logging.info(f"✅ Merge into {full_table_name} completed successfully.")

    except Exception as e:
        logging.error(f"FATAL: Failed during MERGE phase for '{full_table_name}'.", exc_info=True)
        raise


def merge_delta_table_with_scd(
    spark: SparkSession,
    arriving_df: DataFrame,
    db_name: str,
    table_name: str,
    business_key: str,
    business_key_version: str,
    tracked_attribute_cols: dict[str, int],
    business_cols: list[str],
    metadata_cols: list[str] = ["_ingested_at", "_inserted_at"],
):
    full_table_name = f"{db_name}.{table_name}"
    """
    Merge a DataFrame into a Delta table using SCD Type 2 merge strategy.

    The merge strategy is as follows:
    1. If the record is new (i.e., not present in the target table), insert it into the target table.
    2. If the record is expired (i.e., its version is different from the latest version in the target table), update its effective_end_date to the current timestamp.
    3. If the record is updated (i.e., its version is different from the latest version in the target table), update its effective_start_date to the current timestamp and effective_end_date to NULL.

    Args:
        spark: The active SparkSession.
        arriving_df: The DataFrame to be merged into the target table.
        db_name: The name of the database.
        table_name: The name of the table.
        business_key: The name of the surrogate key column.
        business_key_version: The name of the surrogate key version column.
        tracked_attribute_cols: A dictionary of column names to their corresponding version numbers.
        metadata_cols: A list of metadata column names (default=["_ingested_at", "_inserted_at"]).
    """
    table_path = f"{base}/{db_name}/{table_name}"


    now = current_timestamp()

    try:
        if not spark.catalog.tableExists(full_table_name):
            logging.info(f"Table {full_table_name} does not exist. Creating a new table...")
            # Create the table
            arriving_df = arriving_df.withColumn(
                "effective_start_date", now
            ).withColumn(
                "effective_end_date", lit(None).cast("timestamp")
            )

            arriving_df = arriving_df.select(
                sha2(
                    concat_ws("|", col(business_key).cast("string"), now.cast("string")),
                    256
                ).alias(business_key_version),
                *arriving_df.columns,
            )

            arriving_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(table_path)
            spark.sql(f"CREATE TABLE {full_table_name} USING DELTA LOCATION '{table_path}'")
            logging.info(f"Table {full_table_name} created successfully. Merge operation skipped as table was just created.")
            return
        
        logging.info(f"--- Preparing to MERGE with SCD into Delta table: {full_table_name} ---")
        
        # Align the schemas, mainly for merging data from diff sources
        align_schema(spark, arriving_df, full_table_name, table_path)
        
        target_table = DeltaTable.forPath(spark, table_path)
        existing_df = target_table.toDF().alias("existing").filter("effective_end_date IS NULL")
        arriving_df = arriving_df.alias("arriving")

        # Perform the left join
        compilation_df = arriving_df.join(
            existing_df, on=business_key, how="left"
        )

        logging.info(f"Existing records: {existing_df.count()}")
        logging.info(f"Arriving records: {arriving_df.count()}")

        logging.info(f"Records to be merged: {compilation_df.count()}")

        # Apply the merge strategies
        all_cols = set(existing_df.columns) | set(arriving_df.columns)

        # Build the hash
        def build_hash(df_alias, tracked_cols):
            return sha2(
                concat_ws(
                    "|",
                    *[
                        lower(trim(coalesce(col(f"{df_alias}.{c}").cast("string"), lit("NULL"))))
                        for c in tracked_cols
                    ]
                ),
                256
            )

        compilation_df = compilation_df.withColumn(
            "arriving_hash",
            build_hash("arriving", tracked_attribute_cols)
        ).withColumn(
            "existing_hash",
            build_hash("existing", tracked_attribute_cols)
        )

        compilation_df.filter(
            (col("arriving.length_feet") == 4300) &
            (col("arriving.width_feet") == 150) &
            (col("arriving.runway_name") == "22") &
            (col("arriving.elevation_feet") == 692)
        ).show(10)

        logging.info(f"Preparing expired and new records...")

        # Identify expired records
        expired_records = compilation_df.filter(
            (col(f"existing.{business_key_version}").isNotNull()) &
            (col("arriving_hash") != col("existing_hash"))
        ).select(
            col(f"existing.{business_key_version}").alias(business_key_version),
            now.alias("effective_end_date")
        )

        # Identify new records, including new version of expired
        new_records = compilation_df.filter(
            (col(f"existing.{business_key_version}").isNull() | # for new records
            (col("arriving_hash") != col("existing_hash"))) # for updated records
        )

        logging.info(f"Expired count: {expired_records.count()}, New count: {new_records.count()}")

        # Business cols mainly for this
        select_exprs = [col(business_key)]
        arriving_cols = list(tracked_attribute_cols.keys()) + metadata_cols + business_cols
        skip_cols = [business_key_version, business_key, "effective_start_date", "effective_end_date"]

        
        for column_name in all_cols:
            if column_name in skip_cols:
                continue
            if column_name in arriving_cols:
                # for scd2 we respect new data nulls
                expr = col(f"arriving.{column_name}")
            else:
                # always respect existing data otherwise
                expr = col(f"existing.{column_name}")
            
            select_exprs.append(expr.alias(column_name))

        new_records = new_records.select(*select_exprs)
        new_records = new_records.select(
            sha2(
                concat_ws("|", col(business_key).cast("string"), now.cast("string")),
                256
            ).alias(business_key_version),
            *new_records.columns,
            now.alias("effective_start_date"),
            lit(None).cast("timestamp").alias("effective_end_date")
        )

        all_records = new_records.unionByName(expired_records, allowMissingColumns=True)
        
        all_records.filter(
            (col("length_feet") == 4300) &
            (col("width_feet") == 150) &
            (col("runway_name") == "22") &
            (col("elevation_feet") == 692)
        ).show(10)

        # Perform the merge on version_key - PK of the table
        (
            target_table.alias("target").merge(
                all_records.alias("source"),
                condition=f"target.{business_key_version} = source.{business_key_version}"
            )
            .whenNotMatchedInsertAll()
            .whenMatchedUpdate(
                set={
                    "effective_end_date": "source.effective_end_date"
                }
            ).execute()
        )

        logging.info(f"SCD Type 2 merge into {full_table_name} completed successfully.")

    except Exception as e:
        logging.error(f"FATAL: Failed during WRITE phase for '{full_table_name}'. Error: {e}", exc_info=True)
        raise
    