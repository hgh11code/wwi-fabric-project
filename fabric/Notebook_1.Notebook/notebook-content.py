# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "db08a2e9-f6c8-4a56-8540-94277a004073",
# META       "default_lakehouse_name": "wwi_bronze",
# META       "default_lakehouse_workspace_id": "c64ac1e1-7c4e-48d9-a80e-89d584a3b1c0",
# META       "known_lakehouses": [
# META         {
# META           "id": "db08a2e9-f6c8-4a56-8540-94277a004073"
# META         },
# META         {
# META           "id": "efe3c3ab-bf7f-4683-bdab-5a6cb3a057da"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Read the existing table configuration and extraction progress.
# We will reuse this setup when enabling all 48 tables.
# This cell does not change configuration or watermarks.

display(
    spark.table("ops.table_config")
    .orderBy("SourceSchema", "SourceTable")
)

display(
    spark.table("ops.watermarks")
    .orderBy("SourceSchema", "SourceTable")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect the approved full-load files to find each table's latest change.
# Show the stored timestamp type so we can check precision before setup.
# This cell does not update configuration, files, or watermarks.

from pyspark.sql import functions as F

baseline_batch = "20f2f6b1-aea1-4093-a63f-f649bc5447c8"

baseline = (
    spark.table("ops.batch_log")
    .filter(
        (F.col("BatchID") == baseline_batch)
        & (F.col("AttemptID") == "01")
        & (F.lower(F.col("LoadType")) == "full")
        & (F.col("BatchStatus") == "VALIDATED")
    )
    .select("SourceSchema", "SourceTable", "RawPathPattern")
    .distinct()
)

config = spark.table("ops.table_config").select(
    "SourceSchema", "SourceTable", "WatermarkColumn"
)

tables = baseline.join(
    config, ["SourceSchema", "SourceTable"], "left"
).collect()

if len(tables) != 48:
    raise RuntimeError(f"Expected 48 baseline tables; found {len(tables)}.")

results = []

for table in tables:
    schema = table["SourceSchema"]
    name = table["SourceTable"]
    column = table["WatermarkColumn"]

    # Find the date folders belonging to this approved full-load batch.
# Keep the batch and attempt fixed so other loads are excluded.
    path = (
        f"Files/raw/wwi/{schema}/{name}/load_type=full/"
        f"ingest_year=*/ingest_month=*/ingest_day=*/"
        f"batch_id={baseline_batch}/attempt_id=01/*.parquet"
    )

    df = spark.read.parquet(path)

    if not column or column not in df.columns:
        raise RuntimeError(
            f"{schema}.{name}: watermark column {column!r} is missing."
        )

    maximum = df.agg(F.max(F.col(column)).alias("maximum")).first()["maximum"]

    results.append((
        schema,
        name,
        column,
        df.schema[column].dataType.simpleString(),
        None if maximum is None else str(maximum)
    ))

display(
    spark.createDataFrame(
        results,
        """SourceSchema string, SourceTable string,
           WatermarkColumn string, StoredType string,
           FullLoadMaximum string"""
    ).orderBy("SourceSchema", "SourceTable")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Show the recorded full loads for each source table.
# This only reads the log; it does not change data or watermarks.
display(spark.sql("""
    SELECT SourceSchema, SourceTable, BatchID, AttemptID,
           SourceRowCount, BronzeRowCount, BatchStatus
    FROM ops.batch_log
    WHERE LOWER(LoadType) = 'full'
    ORDER BY SourceSchema, SourceTable
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

APPROVED_BATCH_ID = "20f2f6b1-aea1-4093-a63f-f649bc5447c8"

TABLES = [
    ("Orders", "wwi_silver.sales.orders"),
    ("OrderLines", "wwi_silver.sales.order_lines"),
]

ISO_PATTERN = (
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,7})?$"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

baseline_results = []

for source_table, silver_table in TABLES:
    df = spark.table(silver_table)

    required = {"last_edited_when_source_text", "_batch_id"}
    missing = required - set(df.columns)

    if missing:
        raise RuntimeError(
            f"{silver_table}: missing required columns {sorted(missing)}"
        )

    timestamp_text = F.col("last_edited_when_source_text")

    # Pad fractional seconds to exactly seven digits for exact comparison.
    normalized_timestamp = F.concat(
        F.substring(timestamp_text, 1, 19),
        F.lit("."),
        F.rpad(
            F.regexp_extract(timestamp_text, r"\.(\d{1,7})$", 1),
            7,
            "0"
        )
    )

    invalid_timestamp = (
        timestamp_text.isNull()
        | ~timestamp_text.rlike(ISO_PATTERN)
        | F.col("last_edited_when").isNull()
    )

    wrong_batch = (
        F.col("_batch_id").isNull()
        | (F.col("_batch_id") != APPROVED_BATCH_ID)
    )

    result = df.agg(
        F.count("*").alias("row_count"),
        F.count(F.when(invalid_timestamp, 1)).alias("invalid_timestamps"),
        F.count(F.when(wrong_batch, 1)).alias("wrong_batch_rows"),
        F.max(normalized_timestamp).alias("initial_watermark")
    ).first()

    if (
        result["row_count"] == 0
        or result["invalid_timestamps"] != 0
        or result["wrong_batch_rows"] != 0
    ):
        raise RuntimeError(
            f"{silver_table}: baseline check failed: {result.asDict()}"
        )

    baseline_results.append((
        "Sales",
        source_table,
        result["row_count"],
        "LastEditedWhen",
        result["initial_watermark"],
        APPROVED_BATCH_ID
    ))

display(spark.createDataFrame(
    baseline_results,
    [
        "SourceSchema",
        "SourceTable",
        "BaselineRows",
        "WatermarkColumn",
        "ProposedInitialWatermark",
        "BaselineBatchID"
    ]
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F

WATERMARK_TABLE = "wwi_bronze.ops.watermarks"
INITIAL_WATERMARK = "2016-05-31T12:00:00.0000000"
BASELINE_BATCH = "20f2f6b1-aea1-4093-a63f-f649bc5447c8"

watermark_seed = spark.createDataFrame(
    [
        ("AZURE_SQL_WWI", "free-sql-db-7745991", "Sales", "Orders"),
        ("AZURE_SQL_WWI", "free-sql-db-7745991", "Sales", "OrderLines"),
    ],
    ["SourceSystem", "SourceDatabase", "SourceSchema", "SourceTable"]
).withColumn(
    "WatermarkColumn", F.lit("LastEditedWhen")
).withColumn(
    "PreviousWatermark", F.lit(None).cast("string")
).withColumn(
    "CurrentWatermark", F.lit(INITIAL_WATERMARK)
).withColumn(
    "LastSuccessfulBatchID", F.lit(BASELINE_BATCH)
).withColumn(
    "WatermarkStatus", F.lit("INITIALIZED")
).withColumn(
    "UpdatedAtUTC", F.current_timestamp()
)

watermark_keys = [
    "SourceSystem", "SourceDatabase", "SourceSchema", "SourceTable"
]

existing_scope = spark.table(WATERMARK_TABLE).join(
    watermark_seed.select(*watermark_keys),
    watermark_keys,
    "inner"
)

if existing_scope.groupBy(*watermark_keys).count().filter(
    F.col("count") > 1
).take(1):
    raise RuntimeError("Duplicate watermark records found. Initialization stopped.")

display(watermark_seed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

merge_condition = " AND ".join(
    f"target.{column} = source.{column}"
    for column in watermark_keys
)

(
    DeltaTable.forName(spark, WATERMARK_TABLE)
    .alias("target")
    .merge(watermark_seed.alias("source"), merge_condition)
    .whenNotMatchedInsertAll()
    .execute()
)

saved_watermarks = spark.table(WATERMARK_TABLE).join(
    watermark_seed.select(*watermark_keys),
    watermark_keys,
    "inner"
)

if saved_watermarks.count() != 2:
    raise RuntimeError("Expected exactly two watermark records.")

if saved_watermarks.filter(
    F.col("CurrentWatermark").isNull()
    | (F.col("WatermarkColumn") != "LastEditedWhen")
    | F.col("WatermarkColumn").isNull()
).take(1):
    raise RuntimeError("Invalid existing watermark configuration. Review required.")

display(saved_watermarks.orderBy("SourceTable"))

print("Two watermark records are present. Existing progress was preserved.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
