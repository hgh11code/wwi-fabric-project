# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "efe3c3ab-bf7f-4683-bdab-5a6cb3a057da",
# META       "default_lakehouse_name": "wwi_silver",
# META       "default_lakehouse_workspace_id": "c64ac1e1-7c4e-48d9-a80e-89d584a3b1c0",
# META       "known_lakehouses": [
# META         {
# META           "id": "efe3c3ab-bf7f-4683-bdab-5a6cb3a057da"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
bronze_batch_id = ""
attempt_id = "01"
silver_run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import datetime, timezone

manifest_directory = (
    "Files/manifests/wwi/silver_incremental/"
    f"batch_id={bronze_batch_id}/"
    f"silver_run_id={silver_run_id}"
)

process_manifest_path = (
    f"{manifest_directory}/process_results.json"
)

validation_manifest_path = (
    f"{manifest_directory}/validation_results.json"
)

success_manifest_path = (
    f"{manifest_directory}/_SUCCESS.json"
)


def read_json_file(path):
    binary_row = (
        spark.read
        .format("binaryFile")
        .load(path)
        .select("content")
        .first()
    )

    if binary_row is None:
        raise RuntimeError(f"Cannot read: {path}")

    content = bytes(
        binary_row["content"]
    ).decode("utf-8-sig").strip()

    if not content:
        raise RuntimeError(f"File is empty: {path}")

    return json.loads(content)


for required_path in [
    process_manifest_path,
    validation_manifest_path
]:
    if not notebookutils.fs.exists(required_path):
        raise RuntimeError(
            f"Required manifest is missing: {required_path}"
        )


process_manifest = read_json_file(
    process_manifest_path
)

validation_manifest = read_json_file(
    validation_manifest_path
)


if process_manifest.get("Status") != "PROCESSED":
    raise RuntimeError(
        "Silver processing status is not PROCESSED"
    )

if validation_manifest.get("Status") != "VALIDATED":
    raise RuntimeError(
        "Silver validation status is not VALIDATED"
    )

if process_manifest.get("ProcessedTableCount") != 48:
    raise RuntimeError(
        "Processing manifest does not contain 48 tables"
    )

if validation_manifest.get("PassedTableCount") != 48:
    raise RuntimeError(
        "Validation manifest does not contain 48 passed tables"
    )

if validation_manifest.get("FailedTableCount") != 0:
    raise RuntimeError(
        "Validation manifest contains failed tables"
    )

print("Processing and validation manifests verified")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F

process_tables = {
    (row["SourceSchema"], row["SourceTable"], row["SilverTarget"])
    for row in process_manifest["Tables"]
}
validation_tables = {
    (row["SourceSchema"], row["SourceTable"], row["SilverTarget"])
    for row in validation_manifest["Tables"]
}

if len(process_tables) != 48:
    raise RuntimeError(
        f"Processing manifest contains {len(process_tables)} unique tables"
    )
if process_tables != validation_tables:
    raise RuntimeError(
        "Processing/validation table mismatch. "
        f"Missing={process_tables - validation_tables}; "
        f"Unexpected={validation_tables - process_tables}"
    )

LEGACY_RECOVERY_IDENTITY = (
    "022b44d5-41b8-4900-a325-df07e0172d24",
    "01",
    "b2db53da-f54d-4954-9ced-d881b4b2bbe2",
)


def verify_current_silver_state():
    rows = process_manifest["Tables"]
    has_versions = all(
        row.get("DeltaVersionAfterProcessing") is not None
        for row in rows
    )

    if has_versions:
        for row in rows:
            latest = (
                DeltaTable.forName(spark, row["SilverTarget"])
                .history(1)
                .select("version")
                .first()
            )
            actual_version = None if latest is None else int(latest["version"])
            expected_version = int(row["DeltaVersionAfterProcessing"])
            if actual_version != expected_version:
                raise RuntimeError(
                    f"{row['SilverTarget']} changed after this Silver run: "
                    f"expected Delta version {expected_version}, "
                    f"found {actual_version}"
                )
        return "DELTA_VERSION_LOCK"

    requested_identity = (bronze_batch_id, attempt_id, silver_run_id)
    if requested_identity != LEGACY_RECOVERY_IDENTITY:
        raise RuntimeError(
            "The processing manifest predates Delta-version capture. "
            "Legacy finalization is allowed only for the preserved recovery batch."
        )

    snapshots = [row for row in rows if row.get("Method") == "SNAPSHOT"]
    if len(snapshots) != 3:
        raise RuntimeError(
            f"Legacy recovery requires three snapshot sentinels; found {len(snapshots)}"
        )

    for row in snapshots:
        frame = spark.table(row["SilverTarget"])
        for required_column in ["_silver_run_id", "_batch_id"]:
            if required_column not in frame.columns:
                raise RuntimeError(
                    f"{row['SilverTarget']} lacks {required_column}"
                )
        total_rows = frame.count()
        invalid_rows = frame.filter(
            (~F.col("_silver_run_id").eqNullSafe(F.lit(silver_run_id)))
            | (~F.col("_batch_id").eqNullSafe(F.lit(bronze_batch_id)))
        ).limit(1).count()
        if total_rows <= 0 or invalid_rows:
            raise RuntimeError(
                f"{row['SilverTarget']} no longer represents the legacy recovery run"
            )

    return "LEGACY_SNAPSHOT_SENTINELS"


state_guard_mode = verify_current_silver_state()
print("All 48 table identities match")
print("Silver state guard:", state_guard_mode)

process_tables = {
    (
        row["SourceSchema"],
        row["SourceTable"],
        row["SilverTarget"]
    )
    for row in process_manifest["Tables"]
}

validation_tables = {
    (
        row["SourceSchema"],
        row["SourceTable"],
        row["SilverTarget"]
    )
    for row in validation_manifest["Tables"]
}

if len(process_tables) != 48:
    raise RuntimeError(
        f"Processing manifest contains "
        f"{len(process_tables)} unique tables"
    )

if process_tables != validation_tables:
    missing_validation = (
        process_tables - validation_tables
    )

    unexpected_validation = (
        validation_tables - process_tables
    )

    raise RuntimeError(
        "Processing/validation table mismatch. "
        f"Missing={missing_validation}; "
        f"Unexpected={unexpected_validation}"
    )

print("All 48 table identities match")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE SCHEMA IF NOT EXISTS wwi_silver.ops
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS wwi_silver.ops.silver_batch_log
(
    BronzeBatchID STRING,
    BronzeAttemptID STRING,
    SilverRunID STRING,
    BatchStatus STRING,
    ProcessedTableCount INT,
    ValidatedTableCount INT,
    FailedTableCount INT,
    ProcessManifestPath STRING,
    ValidationManifestPath STRING,
    SuccessManifestPath STRING,
    CompletedAtUTC STRING
)
USING DELTA
""")

exact_rows = (
    spark.table("wwi_silver.ops.silver_batch_log")
    .filter(F.col("BronzeBatchID") == bronze_batch_id)
    .filter(F.col("BronzeAttemptID") == attempt_id)
    .filter(F.col("SilverRunID") == silver_run_id)
    .limit(2)
    .collect()
)
if len(exact_rows) > 1:
    raise RuntimeError("Duplicate Silver completion rows exist for the exact identity")

preserved_timestamps = []
if exact_rows:
    existing = exact_rows[0]
    expected_existing = {
        "BatchStatus": "COMPLETED",
        "ProcessedTableCount": 48,
        "ValidatedTableCount": 48,
        "FailedTableCount": 0,
        "ProcessManifestPath": process_manifest_path,
        "ValidationManifestPath": validation_manifest_path,
        "SuccessManifestPath": success_manifest_path
    }
    for field, expected_value in expected_existing.items():
        if str(existing[field]) != str(expected_value):
            raise RuntimeError(
                f"Existing Silver completion {field} mismatch: "
                f"expected {expected_value!r}, got {existing[field]!r}"
            )
    if not existing["CompletedAtUTC"]:
        raise RuntimeError("Existing Silver completion has no timestamp")
    preserved_timestamps.append(str(existing["CompletedAtUTC"]))

if notebookutils.fs.exists(success_manifest_path):
    existing_success = read_json_file(success_manifest_path)
    expected_success = {
        "BronzeBatchID": bronze_batch_id,
        "BronzeAttemptID": attempt_id,
        "SilverRunID": silver_run_id,
        "Status": "COMPLETED",
        "ProcessedTableCount": 48,
        "ValidatedTableCount": 48,
        "FailedTableCount": 0,
        "ProcessManifestPath": process_manifest_path,
        "ValidationManifestPath": validation_manifest_path
    }
    for field, expected_value in expected_success.items():
        if str(existing_success.get(field)) != str(expected_value):
            raise RuntimeError(
                f"Existing Silver success manifest {field} mismatch"
            )
    marker_timestamp = existing_success.get("CompletedAtUTC")
    if not marker_timestamp:
        raise RuntimeError("Existing Silver success manifest has no timestamp")
    preserved_timestamps.append(str(marker_timestamp))

if len(set(preserved_timestamps)) > 1:
    raise RuntimeError(
        "Existing Silver completion row and success manifest disagree on timestamp"
    )

completed_at_utc = (
    preserved_timestamps[0]
    if preserved_timestamps
    else datetime.now(timezone.utc).isoformat()
)

# Recheck immediately before publishing completion so an old run cannot
# relabel Silver tables changed after its processing manifest was written.
verify_current_silver_state()

batch_record_df = spark.createDataFrame([{
    "BronzeBatchID": bronze_batch_id,
    "BronzeAttemptID": attempt_id,
    "SilverRunID": silver_run_id,
    "BatchStatus": "COMPLETED",
    "ProcessedTableCount": 48,
    "ValidatedTableCount": 48,
    "FailedTableCount": 0,
    "ProcessManifestPath": process_manifest_path,
    "ValidationManifestPath": validation_manifest_path,
    "SuccessManifestPath": success_manifest_path,
    "CompletedAtUTC": completed_at_utc
}])

(
    DeltaTable.forName(spark, "wwi_silver.ops.silver_batch_log")
    .alias("target")
    .merge(
        batch_record_df.alias("source"),
        """
        target.BronzeBatchID = source.BronzeBatchID
        AND target.BronzeAttemptID = source.BronzeAttemptID
        AND target.SilverRunID = source.SilverRunID
        """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

success_manifest = {
    "BronzeBatchID": bronze_batch_id,
    "BronzeAttemptID": attempt_id,
    "SilverRunID": silver_run_id,
    "Status": "COMPLETED",
    "ProcessedTableCount": 48,
    "ValidatedTableCount": 48,
    "FailedTableCount": 0,
    "CompletedAtUTC": completed_at_utc,
    "ProcessManifestPath": process_manifest_path,
    "ValidationManifestPath": validation_manifest_path
}

notebookutils.fs.put(
    success_manifest_path,
    json.dumps(success_manifest, indent=2),
    True
)
if not notebookutils.fs.exists(success_manifest_path):
    raise RuntimeError("Silver _SUCCESS.json was not written")

print("SILVER BATCH COMPLETED")
print("Bronze batch:", bronze_batch_id)
print("Silver run:", silver_run_id)
print("Completed at:", completed_at_utc)
print("Validated tables: 48")
print("Success marker:", success_manifest_path)

notebookutils.notebook.exit(json.dumps({
    "status": "COMPLETED",
    "bronze_batch_id": bronze_batch_id,
    "attempt_id": attempt_id,
    "silver_run_id": silver_run_id,
    "completed_at_utc": completed_at_utc,
    "validated_table_count": 48,
    "success_manifest_path": success_manifest_path
}))
from delta.tables import DeltaTable

completed_at_utc = datetime.now(
    timezone.utc
).isoformat()

spark.sql("""
CREATE SCHEMA IF NOT EXISTS wwi_silver.ops
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS
wwi_silver.ops.silver_batch_log
(
    BronzeBatchID STRING,
    BronzeAttemptID STRING,
    SilverRunID STRING,
    BatchStatus STRING,
    ProcessedTableCount INT,
    ValidatedTableCount INT,
    FailedTableCount INT,
    ProcessManifestPath STRING,
    ValidationManifestPath STRING,
    SuccessManifestPath STRING,
    CompletedAtUTC STRING
)
USING DELTA
""")

batch_record_df = spark.createDataFrame([{
    "BronzeBatchID": bronze_batch_id,
    "BronzeAttemptID": attempt_id,
    "SilverRunID": silver_run_id,
    "BatchStatus": "COMPLETED",
    "ProcessedTableCount": 48,
    "ValidatedTableCount": 48,
    "FailedTableCount": 0,
    "ProcessManifestPath": process_manifest_path,
    "ValidationManifestPath": validation_manifest_path,
    "SuccessManifestPath": success_manifest_path,
    "CompletedAtUTC": completed_at_utc
}])

(
    DeltaTable
    .forName(
        spark,
        "wwi_silver.ops.silver_batch_log"
    )
    .alias("target")
    .merge(
        batch_record_df.alias("source"),
        """
        target.BronzeBatchID = source.BronzeBatchID
        AND target.BronzeAttemptID =
            source.BronzeAttemptID
        """
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

success_manifest = {
    "BronzeBatchID": bronze_batch_id,
    "BronzeAttemptID": attempt_id,
    "SilverRunID": silver_run_id,
    "Status": "COMPLETED",
    "ProcessedTableCount": 48,
    "ValidatedTableCount": 48,
    "FailedTableCount": 0,
    "CompletedAtUTC": completed_at_utc,
    "ProcessManifestPath": process_manifest_path,
    "ValidationManifestPath": validation_manifest_path
}

notebookutils.fs.put(
    success_manifest_path,
    json.dumps(
        success_manifest,
        indent=2
    ),
    True
)

if not notebookutils.fs.exists(success_manifest_path):
    raise RuntimeError(
        "Silver _SUCCESS.json was not written"
    )

print("SILVER BATCH COMPLETED")
print("Bronze batch:", bronze_batch_id)
print("Silver run:", silver_run_id)
print("Validated tables: 48")
print("Success marker:", success_manifest_path)

notebookutils.notebook.exit(
    json.dumps({
        "status": "COMPLETED",
        "bronze_batch_id": bronze_batch_id,
        "attempt_id": attempt_id,
        "silver_run_id": silver_run_id,
        "validated_table_count": 48,
        "success_manifest_path": success_manifest_path
    })
)

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
