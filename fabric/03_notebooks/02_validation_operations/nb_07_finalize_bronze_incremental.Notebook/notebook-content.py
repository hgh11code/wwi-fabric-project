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

# Receive the exact extraction plan used by this pipeline run.
# These values must come from the master pipeline.
# Leave the defaults blank to prevent accidental manual execution.

batch_id = ""
attempt_id = "01"
table_list_json = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Temp recovery cell

# import json

# # Read all lines of the uploaded file.
# lines = spark.read.text(
#     "Files/bronze_recovery_input.json"
# ).collect()

# raw = "\n".join(row["value"] for row in lines)
# recovery_input = json.loads(raw)

# # Recover the original batch that completed ingestion.
# batch_id = "022b44d5-41b8-4900-a325-df07e0172d24"
# attempt_id = "01"
# table_list_json = recovery_input["parameters"]["table_list_json"]["value"]

# tables = json.loads(table_list_json)

# assert len(tables) == 48
# assert all(
#     t["UpperWatermark"] == "2016-06-07T23:59:59.9999999"
#     for t in tables
# )

# print("Recovery batch:", batch_id)
# print("Tables:", len(tables))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Require a matching successful receipt and audit log for every planned table.
# Check the extraction boundaries and row counts before changing watermarks.
# Any missing or failed table stops batch completion.

import json
import re
from datetime import datetime, timezone
from pyspark.sql import functions as F
from delta.tables import DeltaTable

if not batch_id or not attempt_id or not table_list_json:
    raise ValueError("Required pipeline parameters are missing.")

for value in [batch_id, attempt_id]:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("Invalid batch or attempt identifier.")

plan = json.loads(table_list_json)

keys = ["SourceSystem", "SourceDatabase", "SourceSchema", "SourceTable"]

def table_key(item):
    return tuple(item[key] for key in keys)

if len(plan) != 48 or len({table_key(p) for p in plan}) != 48:
    raise RuntimeError("Expected exactly 48 unique planned tables.")

root = (
    f"Files/manifests/wwi/bronze_incremental/"
    f"batch_id={batch_id}/attempt_id={attempt_id}"
)

paths = []
for p in plan:
    for field in ["SourceSchema", "SourceTable"]:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", p[field]):
            raise ValueError(f"Invalid table identifier: {p[field]}")
    paths.append(f"{root}/{p['SourceSchema']}/{p['SourceTable']}.json")

# Explicit paths ensure a missing table receipt causes failure.
receipts = [
    json.loads(row["value"])
    for row in spark.read.text(paths).collect()
]

if len(receipts) != 48 or len({table_key(r) for r in receipts}) != 48:
    raise RuntimeError("Expected exactly 48 unique validation receipts.")

receipt_map = {table_key(r): r for r in receipts}

logs = [
    row.asDict()
    for row in (
        spark.table("ops.batch_log")
        .filter(
            (F.col("BatchID") == batch_id)
            & (F.col("AttemptID") == attempt_id)
        )
        .collect()
    )
]

for p in plan:
    key = table_key(p)
    r = receipt_map.get(key)

    if r is None:
        raise RuntimeError(f"Missing receipt: {key}")

    if r["BatchID"] != batch_id or r["AttemptID"] != attempt_id:
        raise RuntimeError(f"Wrong batch in receipt: {key}")

    # Compare all instructions, including queries, bounds and extraction method.
    if any(r.get(field) != value for field, value in p.items()):
        raise RuntimeError(f"Receipt does not match the extraction plan: {key}")

    counts = [
        r.get("SourceRowCount"),
        r.get("CopyRowsRead"),
        r.get("CopyRowsCopied"),
        r.get("BronzeRowCount")
    ]

    if (
        r.get("BatchStatus") != "VALIDATED"
        or r.get("ErrorMessage")
        or any(type(value) is not int or value < 0 for value in counts)
        or len(set(counts)) != 1
    ):
        raise RuntimeError(f"Validation failed: {key}")

    matching_logs = [
        log for log in logs
        if table_key(log) == key
        and log["ValidationAttemptID"] == r["ValidationAttemptID"]
    ]

    if len(matching_logs) != 1:
        raise RuntimeError(f"Missing or ambiguous audit log: {key}")

    log = matching_logs[0]
    if (
        log["BatchStatus"] != "VALIDATED"
        or log["LoadType"] != p["LoadType"]
        or log["SourceRowCount"] != counts[0]
        or log["BronzeRowCount"] != counts[0]
        or log["RowCountDifference"] != 0
        or log["RawPathPattern"] != r["RawPath"]
    ):
        raise RuntimeError(f"Audit log does not match receipt: {key}")

print("All 48 table receipts and audit logs passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Advance all incremental watermarks in one Delta MERGE.
# Permit a retry of this same finalized batch, but reject changed starting points.
# Save the batch-completion receipt only after the watermark update succeeds.

incremental = [p for p in plan if p["LoadType"] == "incremental"]
snapshots = [p for p in plan if p["LoadType"] == "snapshot"]

if len(incremental) != 45 or len(snapshots) != 3:
    raise RuntimeError("Expected 45 incremental tables and 3 snapshots.")

current_rows = [
    row.asDict() for row in spark.table("ops.watermarks").collect()
]
if len({table_key(r) for r in current_rows}) != len(current_rows):
    raise RuntimeError("Duplicate watermark records.")

current = {table_key(r): r for r in current_rows}
updates = []

for p in incremental:
    key = table_key(p)
    w = current.get(key)
    lower = p["LowerWatermark"]
    upper = p["UpperWatermark"]

    if not lower or not upper or lower > upper:
        raise RuntimeError(f"Invalid extraction bounds: {key}")

    if w is None or w["WatermarkColumn"] != p["WatermarkColumn"]:
        raise RuntimeError(f"Missing or incompatible watermark: {key}")

    already_committed = (
        w["CurrentWatermark"] == upper
        and w["LastSuccessfulBatchID"] == batch_id
    )

    if w["CurrentWatermark"] != lower and not already_committed:
        raise RuntimeError(f"Watermark changed since preparation: {key}")

    updates.append((
        *key,
        p["WatermarkColumn"],
        lower,
        upper,
        batch_id
    ))

update_df = spark.createDataFrame(
    updates,
    """
    SourceSystem string, SourceDatabase string,
    SourceSchema string, SourceTable string,
    WatermarkColumn string, LowerWatermark string,
    UpperWatermark string, BatchID string
    """
)

match = " AND ".join(f"t.{key} = s.{key}" for key in keys)

already = """
    t.CurrentWatermark = s.UpperWatermark
    AND t.LastSuccessfulBatchID = s.BatchID
"""

valid = f"""
    t.WatermarkColumn = s.WatermarkColumn
    AND (t.CurrentWatermark = s.LowerWatermark OR ({already}))
"""

(
    DeltaTable.forName(spark, "ops.watermarks")
    .alias("t")
    .merge(update_df.alias("s"), match)
    .whenMatchedUpdate(set={
        "PreviousWatermark": (
            f"CASE WHEN {already} THEN t.PreviousWatermark "
            "ELSE s.LowerWatermark END"
        ),
        "CurrentWatermark": (
            f"CASE WHEN {valid} THEN s.UpperWatermark "
            "ELSE CAST(raise_error('Watermark changed during finalization') "
            "AS STRING) END"
        ),
        "LastSuccessfulBatchID": "s.BatchID",
        "WatermarkStatus": "'ACTIVE'",
        "UpdatedAtUTC": "current_timestamp()"
    })
    .execute()
)

# Verify the committed state before publishing batch success.
after = {
    table_key(row.asDict()): row.asDict()
    for row in spark.table("ops.watermarks").collect()
}

for p in incremental:
    w = after.get(table_key(p))
    if (
        w is None
        or w["CurrentWatermark"] != p["UpperWatermark"]
        or w["LastSuccessfulBatchID"] != batch_id
    ):
        raise RuntimeError("Watermark verification failed; batch not approved.")

completion = {
    "BatchID": batch_id,
    "AttemptID": attempt_id,
    "Status": "COMPLETED",
    "ValidatedTables": len(receipts),
    "IncrementalTables": len(incremental),
    "SnapshotTables": len(snapshots),
    "CompletedAtUTC": datetime.now(timezone.utc).isoformat(),
    "Tables": receipts
}

notebookutils.fs.put(
    f"{root}/_SUCCESS.json",
    json.dumps(completion),
    True
)

print("BATCH COMPLETED: 48 validated tables; 45 watermarks finalized.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
