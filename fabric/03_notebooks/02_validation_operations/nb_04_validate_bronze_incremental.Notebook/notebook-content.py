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
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Pipeline inputs identify the exact batch and attempt to validate.
# These defaults point to our already validated batch for manual testing.
# The pipeline will replace these values when it runs this notebook.

batch_id = "988e3889-17c4-479d-a70d-f6a77592c148"
attempt_id = "01"
ingestion_date_utc = "2026-09-16T00:00:00Z"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build the Bronze folder from the supplied ingestion date and batch.
# Validate only Orders and OrderLines for this first incremental window.
# Expected counts apply to our June 1 batch.

from datetime import datetime

ingestion_date = datetime.fromisoformat(
    ingestion_date_utc.replace("Z", "+00:00")
)

batch_folder = (
    "load_type=incremental/"
    f"ingest_year={ingestion_date:%Y}/"
    f"ingest_month={ingestion_date:%m}/"
    f"ingest_day={ingestion_date:%d}/"
    f"batch_id={batch_id}/attempt_id={attempt_id}"
)

tables_to_validate = [
    {"table": "Orders", "key": "OrderID", "rows": 10, "columns": 16},
    {"table": "OrderLines", "key": "OrderLineID", "rows": 27, "columns": 12},
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PURPOSE: Check actual Bronze files, row counts, keys and timestamps.
# Timestamp columns must remain strings, preserving the raw SQL values.
# This cell reads data only; it does not write logs or advance watermarks.

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

results = []

for config in tables_to_validate:
    table = config["table"]
    key = config["key"]

    path = f"Files/raw/wwi/Sales/{table}/{batch_folder}"

    try:
        df = (
            spark.read
            .option("recursiveFileLookup", "true")
            .parquet(path)
        )

        required_columns = {
            key, "PickingCompletedWhen", "LastEditedWhen"
        }
        missing = required_columns - set(df.columns)

        if missing:
            raise ValueError(f"Missing columns: {sorted(missing)}")

        row_count = df.count()
        file_count = len(df.inputFiles())
        column_count = len(df.columns)

        null_keys = df.filter(F.col(key).isNull()).count()

        duplicate_keys = (
            df.groupBy(key)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        timestamps_are_strings = all(
            isinstance(df.schema[column].dataType, StringType)
            for column in ["PickingCompletedWhen", "LastEditedWhen"]
        )

        # This first batch uses whole-second boundaries.
        # Raw timestamp strings are retained; parsing is only for validation.
        parsed_change = F.expr(
            "try_cast(LastEditedWhen AS TIMESTAMP)"
        )

        invalid_change_rows = df.filter(
            parsed_change.isNull()
            | (
                parsed_change
                <= F.lit("2016-05-31 12:00:00").cast("timestamp")
            )
            | (
                parsed_change
                > F.lit("2016-06-01 12:00:00").cast("timestamp")
            )
        ).count()

        checks = {
            "expected_rows": row_count == config["rows"],
            "expected_columns": column_count == config["columns"],
            "files_present": file_count > 0,
            "no_null_keys": null_keys == 0,
            "unique_keys": duplicate_keys == 0,
            "raw_timestamp_strings": timestamps_are_strings,
            "change_window": invalid_change_rows == 0,
        }

        failed_checks = [
            name for name, passed in checks.items() if not passed
        ]

        results.append({
            "SourceTable": f"Sales.{table}",
            "BronzeRowCount": row_count,
            "ParquetFileCount": file_count,
            "BronzeColumnCount": column_count,
            "Status": "PASSED" if not failed_checks else "FAILED",
            "Details": ", ".join(failed_checks),
            "RawPath": path,
        })

    except Exception as error:
        results.append({
            "SourceTable": f"Sales.{table}",
            "BronzeRowCount": -1,
            "ParquetFileCount": -1,
            "BronzeColumnCount": -1,
            "Status": "FAILED",
            "Details": str(error),
            "RawPath": path,
        })

display(spark.createDataFrame(results))

if any(result["Status"] != "PASSED" for result in results):
    raise RuntimeError(
        "Bronze validation failed. Do not log success or advance watermarks."
    )

print("Both tables passed Bronze validation.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PURPOSE: Save the two successful Bronze validation results.
# Reuses ops.batch_log and prevents duplicate entries on ordinary reruns.
# Does not modify Parquet files, Silver tables or watermarks.

from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import functions as F

log_table = "ops.batch_log"
config_table = "ops.table_config"

source_system = "AZURE_SQL_WWI"
source_database = "free-sql-db-7745991"
validation_attempt_id = "01"

expected_rows = {
    "Sales.Orders": 10,
    "Sales.OrderLines": 27,
}

# Require both successful results from Cell 2.
if (
    len(results) != 2
    or {r["SourceTable"] for r in results} != set(expected_rows)
    or any(r["Status"] != "PASSED" for r in results)
):
    raise RuntimeError("Run Cells 1–2 successfully before logging.")

# Read actual configured table classifications.
config_rows = (
    spark.table(config_table)
    .filter(
        (F.col("SourceSystem") == source_system)
        & (F.col("SourceDatabase") == source_database)
        & (F.col("SourceSchema") == "Sales")
        & F.col("SourceTable").isin("Orders", "OrderLines")
    )
    .select("SourceTable", "TableType")
    .collect()
)

if (
    len(config_rows) != 2
    or {r["SourceTable"] for r in config_rows}
       != {"Orders", "OrderLines"}
):
    raise RuntimeError("Expected one configuration row for each table.")

table_types = {
    r["SourceTable"]: r["TableType"] for r in config_rows
}

now_utc = datetime.now(timezone.utc)
records = []

for result in results:
    schema_name, table_name = result["SourceTable"].split(".", 1)

    expected_path = (
        f"Files/raw/wwi/{schema_name}/{table_name}/{batch_folder}"
    )

    if (
        result["RawPath"] != expected_path
        or result["BronzeRowCount"]
           != expected_rows[result["SourceTable"]]
    ):
        raise RuntimeError("Validation results do not match this batch.")

    records.append({
        "BatchID": batch_id,
        "AttemptID": attempt_id,
        "ValidationAttemptID": validation_attempt_id,
        "LoadType": "INCREMENTAL",
        "SourceSystem": source_system,
        "SourceDatabase": source_database,
        "SourceSchema": schema_name,
        "SourceTable": table_name,
        "TableType": table_types[table_name],
        "SourceRowCount": expected_rows[result["SourceTable"]],
        "BronzeRowCount": result["BronzeRowCount"],
        "RowCountDifference": (
            expected_rows[result["SourceTable"]]
            - result["BronzeRowCount"]
        ),
        "ParquetFileCount": result["ParquetFileCount"],
        "BronzeColumnCount": result["BronzeColumnCount"],
        "RawPathPattern": result["RawPath"] + "/*.parquet",
        "BatchStatus": "VALIDATED",
        "ErrorMessage": None,
        "ValidatedAtUTC": now_utc,
        "LoggedAtUTC": now_utc,
    })

# Use the existing log schema instead of recreating the table.
target_schema = spark.table(log_table).schema

if set(target_schema.fieldNames()) != set(records[0]):
    raise RuntimeError(
        "The live batch_log schema differs from the documented schema. "
        "Run spark.table('ops.batch_log').printSchema() and share it."
    )

new_logs = spark.createDataFrame(records, schema=target_schema)

key_columns = [
    "BatchID", "AttemptID", "ValidationAttemptID",
    "LoadType", "SourceSystem", "SourceDatabase",
    "SourceSchema", "SourceTable",
]

# Refuse to silently accept conflicting or duplicate existing receipts.
existing = spark.table(log_table).join(
    new_logs.select(*key_columns), key_columns, "inner"
)

if (
    existing.groupBy(*key_columns)
    .count()
    .filter(F.col("count") > 1)
    .count()
):
    raise RuntimeError("Duplicate existing log entries need inspection.")

comparison_columns = [
    c for c in new_logs.columns
    if c not in ["ValidatedAtUTC", "LoggedAtUTC"]
]

if (
    existing.select(*comparison_columns)
    .exceptAll(new_logs.select(*comparison_columns))
    .count()
):
    raise RuntimeError(
        "An existing receipt conflicts with these results. Nothing written."
    )

match_condition = " AND ".join(
    f"t.`{column}` = s.`{column}`" for column in key_columns
)

(
    DeltaTable.forName(spark, log_table)
    .alias("t")
    .merge(new_logs.alias("s"), match_condition)
    .whenNotMatchedInsertAll()
    .execute()
)

saved_logs = (
    spark.table(log_table)
    .join(new_logs.select(*key_columns), key_columns, "inner")
)

if saved_logs.count() != 2:
    raise RuntimeError("Expected exactly two saved validation entries.")

display(
    saved_logs.select(
        "BatchID",
        "SourceSchema",
        "SourceTable",
        "SourceRowCount",
        "BronzeRowCount",
        "RowCountDifference",
        "BatchStatus",
        "RawPathPattern",
    )
)

print("Both validated table receipts are saved.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect the saved extraction boundaries for our two tables.
# Confirm the successful batch receipts before advancing watermarks.
# This cell only reads data; it changes nothing.

spark.table("ops.watermarks").printSchema()

display(spark.sql("""
    SELECT *
    FROM ops.watermarks
    WHERE SourceSchema = 'Sales'
      AND SourceTable IN ('Orders', 'OrderLines')
"""))

display(spark.sql("""
    SELECT BatchID, AttemptID, SourceTable,
           SourceRowCount, BronzeRowCount, BatchStatus
    FROM ops.batch_log
    WHERE BatchID = '988e3889-17c4-479d-a70d-f6a77592c148'
      AND AttemptID = '01'
      AND LoadType = 'INCREMENTAL'
      AND SourceSchema = 'Sales'
      AND SourceTable IN ('Orders', 'OrderLines')
"""))

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
