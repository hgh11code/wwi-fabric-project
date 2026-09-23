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

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Receive the table instructions and counts from the child pipeline.
# Blank defaults prevent accidentally validating an unrelated batch.
# Run this notebook through the pipeline after configuring its parameters.

table_spec_json = ""
batch_id = ""
attempt_id = "01"
ingestion_date_utc = ""
source_row_count = ""
copy_rows_read = ""
copy_rows_copied = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Compare the expected source count, Copy output, and actual Parquet count.
# Record validation in ops.batch_log and save the extraction bounds.
# This notebook never advances watermarks.

import json
import re
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

# Identify missing inputs by name without printing their contents.
# A row count of zero is valid and must not be treated as missing.
required = {
    "table_spec_json": table_spec_json,
    "batch_id": batch_id,
    "attempt_id": attempt_id,
    "ingestion_date_utc": ingestion_date_utc,
    "source_row_count": source_row_count,
    "copy_rows_read": copy_rows_read,
    "copy_rows_copied": copy_rows_copied
}

missing = [
    name for name, value in required.items()
    if value is None or str(value).strip() == ""
]

if missing:
    raise ValueError(
        "Missing pipeline parameters: " + ", ".join(missing)
    )

spec = json.loads(table_spec_json)
schema = spec["SourceSchema"]
table = spec["SourceTable"]
load_type = spec["LoadType"]

for value in [schema, table, batch_id, attempt_id]:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError(f"Invalid path component: {value!r}")

if load_type not in {"incremental", "snapshot"}:
    raise ValueError(f"Unexpected load type: {load_type}")

expected = int(source_row_count)
rows_read = int(copy_rows_read)
rows_copied = int(copy_rows_copied)

if min(expected, rows_read, rows_copied) < 0:
    raise ValueError("Row counts cannot be negative.")

ingested = datetime.fromisoformat(
    ingestion_date_utc.replace("Z", "+00:00")
)
if ingested.tzinfo is None:
    raise ValueError("Ingestion date must include its UTC timezone.")

ingested = ingested.astimezone(timezone.utc)

folder = (
    f"Files/raw/wwi/{schema}/{table}/load_type={load_type}/"
    f"ingest_year={ingested:%Y}/ingest_month={ingested:%m}/"
    f"ingest_day={ingested:%d}/batch_id={batch_id}/"
    f"attempt_id={attempt_id}"
)

file_path = f"{folder}/data.parquet"
validation_id = str(uuid.uuid4())
actual = None
column_count = None
file_count = 0
error = None

try:
    if notebookutils.fs.exists(folder):
        files = [
            item for item in notebookutils.fs.ls(folder)
            if item.name.lower().endswith(".parquet")
        ]
        file_count = len(files)

        if any(item.name != "data.parquet" for item in files):
            raise RuntimeError("Unexpected Parquet files in the attempt folder.")

    if notebookutils.fs.exists(file_path):
        bronze = spark.read.parquet(file_path)
        actual = bronze.count()
        column_count = len(bronze.columns)
    elif expected == rows_read == rows_copied == 0:
        # An empty extraction may legitimately produce no Parquet file.
        actual = 0
    else:
        raise RuntimeError(f"Expected output file is missing: {file_path}")

    if not (expected == rows_read == rows_copied == actual):
        raise RuntimeError(
            f"Count mismatch: source={expected}, read={rows_read}, "
            f"copied={rows_copied}, bronze={actual}"
        )

except Exception as exc:
    error = str(exc)

status = "VALIDATED" if error is None else "FAILED"
now = datetime.now(timezone.utc).replace(tzinfo=None)

record = {
    "BatchID": batch_id,
    "AttemptID": attempt_id,
    "ValidationAttemptID": validation_id,
    "LoadType": load_type,
    "SourceSystem": spec["SourceSystem"],
    "SourceDatabase": spec["SourceDatabase"],
    "SourceSchema": schema,
    "SourceTable": table,
    "TableType": spec["TableType"],
    "SourceRowCount": expected,
    "BronzeRowCount": actual,
    "RowCountDifference": None if actual is None else actual - expected,
    "ParquetFileCount": file_count,
    "BronzeColumnCount": column_count,
    "RawPathPattern": folder,
    "BatchStatus": status,
    "ErrorMessage": error,
    "ValidatedAtUTC": now,
    "LoggedAtUTC": now
}

# Reuse the existing log schema so column types remain consistent.
log_schema = spark.table("ops.batch_log").schema
unknown = set(log_schema.fieldNames()) - set(record)
if unknown:
    raise RuntimeError(f"Unexpected batch_log columns: {sorted(unknown)}")

(
    spark.createDataFrame([record], schema=log_schema)
    .write.format("delta")
    .mode("append")
    .saveAsTable("ops.batch_log")
)

# Keep one current receipt per table and attempt.
# The receipt identifies its matching audit-log validation record.
receipt = {
    **spec,
    "BatchID": batch_id,
    "AttemptID": attempt_id,
    "ValidationAttemptID": validation_id,
    "IngestionDateUTC": ingestion_date_utc,
    "SourceRowCount": expected,
    "CopyRowsRead": rows_read,
    "CopyRowsCopied": rows_copied,
    "BronzeRowCount": actual,
    "RawPath": folder,
    "BatchStatus": status,
    "ErrorMessage": error
}

receipt_folder = (
    f"Files/manifests/wwi/bronze_incremental/"
    f"batch_id={batch_id}/attempt_id={attempt_id}/{schema}"
)

notebookutils.fs.mkdirs(receipt_folder)
notebookutils.fs.put(
    f"{receipt_folder}/{table}.json",
    json.dumps(receipt),
    True
)

if error:
    raise RuntimeError(f"{schema}.{table}: {error}")

print(f"{schema}.{table}: VALIDATED — {actual} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
