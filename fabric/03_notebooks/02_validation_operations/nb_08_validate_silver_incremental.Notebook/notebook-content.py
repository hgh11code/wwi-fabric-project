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
# META           "id": "db08a2e9-f6c8-4a56-8540-94277a004073"
# META         },
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
bronze_batch_id = "5732b3bc-802c-4328-b064-ea0cd421af0e"
attempt_id = "01"
silver_run_id = "manual-silver-test-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SILVER_ROOT = (
    "abfss://wwi_analytics"
    "@onelake.dfs.fabric.microsoft.com/"
    "wwi_silver.Lakehouse"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

process_manifest_path = (
    "Files/manifests/wwi/silver_incremental/"
    f"batch_id={bronze_batch_id}/"
    f"silver_run_id={silver_run_id}/"
    "process_results.json"
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
        raise RuntimeError(
            f"Could not read JSON file: {path}"
        )

    text = bytes(
        binary_row["content"]
    ).decode("utf-8-sig").strip()

    if not text:
        raise RuntimeError(
            f"JSON file is empty: {path}"
        )

    return json.loads(text)


if not notebookutils.fs.exists(process_manifest_path):
    raise RuntimeError(
        f"Silver processing manifest is missing: "
        f"{process_manifest_path}"
    )

process_manifest = read_json_file(
    process_manifest_path
)

if process_manifest.get("Status") != "PROCESSED":
    raise RuntimeError(
        "Silver processing manifest is not PROCESSED"
    )

if process_manifest.get("BronzeBatchID") != bronze_batch_id:
    raise RuntimeError(
        "Processing manifest BatchID mismatch"
    )

if process_manifest.get("SilverRunID") != silver_run_id:
    raise RuntimeError(
        "Processing manifest SilverRunID mismatch"
    )

processed_tables = process_manifest.get(
    "Tables",
    []
)

if len(processed_tables) != 48:
    raise RuntimeError(
        f"Expected 48 processed tables, "
        f"found {len(processed_tables)}"
    )

print("Silver processing manifest verified")
print("Processed tables:", len(processed_tables))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_ROOT = (
    "abfss://c64ac1e1-7c4e-48d9-a80e-89d584a3b1c0"
    "@onelake.dfs.fabric.microsoft.com/"
    "db08a2e9-f6c8-4a56-8540-94277a004073"
)

bronze_manifest_root = (
    f"{BRONZE_ROOT}/Files/manifests/wwi/"
    f"bronze_incremental/"
    f"batch_id={bronze_batch_id}/"
    f"attempt_id={attempt_id}"
)


def find_receipt_files(folder_path):
    files = []

    for item in notebookutils.fs.ls(folder_path):
        if item.isDir:
            files.extend(
                find_receipt_files(item.path)
            )
        elif (
            item.path.endswith(".json")
            and not item.path.endswith("_SUCCESS.json")
        ):
            files.append(item.path)

    return files


receipt_paths = sorted(
    find_receipt_files(bronze_manifest_root)
)

if len(receipt_paths) != 48:
    raise RuntimeError(
        f"Expected 48 Bronze receipts, "
        f"found {len(receipt_paths)}"
    )

bronze_receipts = {}

for receipt_path in receipt_paths:
    receipt = read_json_file(receipt_path)

    table_key = (
        receipt["SourceSchema"],
        receipt["SourceTable"]
    )

    if receipt.get("BatchStatus") != "VALIDATED":
        raise RuntimeError(
            f"{table_key}: Bronze receipt is not VALIDATED"
        )

    bronze_receipts[table_key] = receipt


processed_keys = {
    (
        table["SourceSchema"],
        table["SourceTable"]
    )
    for table in processed_tables
}

receipt_keys = set(bronze_receipts.keys())

if processed_keys != receipt_keys:
    missing = receipt_keys - processed_keys
    unexpected = processed_keys - receipt_keys

    raise RuntimeError(
        f"Process/receipt table mismatch. "
        f"Missing={missing}; Unexpected={unexpected}"
    )

print("Bronze receipts verified:", len(bronze_receipts))
print("Processing manifest and Bronze batch match")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from functools import reduce
from pyspark.sql import functions as F


def to_snake_case(column_name):
    value = re.sub(
        r"(.)([A-Z][a-z]+)",
        r"\1_\2",
        column_name
    )
    value = re.sub(
        r"([a-z0-9])([A-Z])",
        r"\1_\2",
        value
    )
    value = re.sub(
        r"[^A-Za-z0-9]+",
        "_",
        value
    )

    return value.strip("_").lower()


def normalize_columns(df):
    result_df = df

    for original_column in df.columns:
        normalized_column = to_snake_case(
            original_column
        )

        if original_column != normalized_column:
            result_df = result_df.withColumnRenamed(
                original_column,
                normalized_column
            )

    return result_df


def parse_merge_keys(key_text):
    if not key_text:
        return []

    return [
        key.strip()
        for key in key_text.split(",")
        if key.strip()
    ]


def add_derived_key_columns(df, merge_keys):
    result_df = df

    for key in merge_keys:
        if (
            key.endswith("_source_text")
            and key not in result_df.columns
        ):
            base_column = key.removesuffix(
                "_source_text"
            )

            if base_column not in result_df.columns:
                raise RuntimeError(
                    f"Cannot derive key {key}; "
                    f"{base_column} is missing"
                )

            result_df = result_df.withColumn(
                key,
                F.col(base_column).cast("string")
            )

    return result_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

validation_results = []

for position, processed in enumerate(
    sorted(
        processed_tables,
        key=lambda row: (
            row["SourceSchema"],
            row["SourceTable"]
        )
    ),
    start=1
):
    source_schema = processed["SourceSchema"]
    source_table = processed["SourceTable"]
    identity = f"{source_schema}.{source_table}"

    silver_target = processed["SilverTarget"]
    method = processed["Method"]
    merge_keys = parse_merge_keys(
        processed.get("MergeKeys")
    )

    receipt = bronze_receipts[
        (source_schema, source_table)
    ]

    raw_path = (
        f"{BRONZE_ROOT}/{receipt['RawPath']}"
    )

    issues = []

    print(f"[{position}/48] Validating {identity}")

    try:
        source_df = normalize_columns(
            spark.read.parquet(raw_path)
        )

        source_df = add_derived_key_columns(
            source_df,
            merge_keys
        )

        target_df = spark.table(silver_target)

        source_count = source_df.count()
        target_count = target_df.count()

        receipt_count = int(
            receipt["BronzeRowCount"]
        )

        process_count = int(
            processed["SourceRows"]
        )

        if source_count != receipt_count:
            issues.append(
                f"Source count {source_count} != "
                f"receipt count {receipt_count}"
            )

        if source_count != process_count:
            issues.append(
                f"Source count {source_count} != "
                f"process count {process_count}"
            )

        source_null_keys = 0
        source_duplicate_keys = 0
        target_null_keys = 0
        target_duplicate_keys = 0
        missing_target_keys = 0

        if method == "SNAPSHOT":
            if target_count != source_count:
                issues.append(
                    f"Snapshot target count {target_count} != "
                    f"source count {source_count}"
                )

        else:
            if not merge_keys:
                issues.append(
                    "Non-snapshot table has no merge keys"
                )
            else:
                missing_source_columns = [
                    key for key in merge_keys
                    if key not in source_df.columns
                ]

                missing_target_columns = [
                    key for key in merge_keys
                    if key not in target_df.columns
                ]

                if missing_source_columns:
                    issues.append(
                        f"Source keys missing: "
                        f"{missing_source_columns}"
                    )

                if missing_target_columns:
                    issues.append(
                        f"Target keys missing: "
                        f"{missing_target_columns}"
                    )

                if (
                    not missing_source_columns
                    and not missing_target_columns
                ):
                    source_null_condition = reduce(
                        lambda left, right: left | right,
                        [
                            F.col(key).isNull()
                            for key in merge_keys
                        ]
                    )

                    target_null_condition = reduce(
                        lambda left, right: left | right,
                        [
                            F.col(key).isNull()
                            for key in merge_keys
                        ]
                    )

                    source_null_keys = (
                        source_df
                        .filter(source_null_condition)
                        .count()
                    )

                    target_null_keys = (
                        target_df
                        .filter(target_null_condition)
                        .count()
                    )

                    source_duplicate_keys = (
                        source_df
                        .groupBy(*merge_keys)
                        .count()
                        .filter(F.col("count") > 1)
                        .limit(1)
                        .count()
                    )

                    target_duplicate_keys = (
                        target_df
                        .groupBy(*merge_keys)
                        .count()
                        .filter(F.col("count") > 1)
                        .limit(1)
                        .count()
                    )

                    source_keys_df = (
                        source_df
                        .select(*merge_keys)
                        .distinct()
                    )

                    target_keys_df = (
                        target_df
                        .select(*merge_keys)
                        .distinct()
                    )

                    missing_target_keys = (
                        source_keys_df
                        .join(
                            target_keys_df,
                            on=merge_keys,
                            how="left_anti"
                        )
                        .count()
                    )

                    if source_null_keys > 0:
                        issues.append(
                            f"Source contains "
                            f"{source_null_keys} null keys"
                        )

                    if target_null_keys > 0:
                        issues.append(
                            f"Target contains "
                            f"{target_null_keys} null keys"
                        )

                    if source_duplicate_keys > 0:
                        issues.append(
                            "Source contains duplicate keys"
                        )

                    if target_duplicate_keys > 0:
                        issues.append(
                            "Target contains duplicate keys"
                        )

                    if missing_target_keys > 0:
                        issues.append(
                            f"{missing_target_keys} source keys "
                            f"are missing from Silver"
                        )

        status = (
            "VALIDATED"
            if not issues
            else "FAILED"
        )

        validation_results.append({
            "SourceSchema": source_schema,
            "SourceTable": source_table,
            "SilverTarget": silver_target,
            "Method": method,
            "SourceRowCount": source_count,
            "SilverRowCount": target_count,
            "SourceNullKeys": source_null_keys,
            "SourceDuplicateKeyFound": source_duplicate_keys,
            "TargetNullKeys": target_null_keys,
            "TargetDuplicateKeyFound": target_duplicate_keys,
            "MissingTargetKeys": missing_target_keys,
            "Status": status,
            "ErrorMessage": "; ".join(issues)
        })

        print("   Status:", status)

    except Exception as error:
        validation_results.append({
            "SourceSchema": source_schema,
            "SourceTable": source_table,
            "SilverTarget": silver_target,
            "Method": method,
            "SourceRowCount": -1,
            "SilverRowCount": -1,
            "SourceNullKeys": -1,
            "SourceDuplicateKeyFound": -1,
            "TargetNullKeys": -1,
            "TargetDuplicateKeyFound": -1,
            "MissingTargetKeys": -1,
            "Status": "FAILED",
            "ErrorMessage": str(error)[:1000]
        })

        print("   FAILED:", str(error))


validation_results_df = spark.createDataFrame(
    validation_results
)

display(
    validation_results_df.orderBy(
        "SourceSchema",
        "SourceTable"
    )
)

passed_count = validation_results_df.filter(
    F.col("Status") == "VALIDATED"
).count()

failed_count = validation_results_df.filter(
    F.col("Status") == "FAILED"
).count()

print("Passed:", passed_count)
print("Failed:", failed_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone

validation_manifest_path = (
    "Files/manifests/wwi/silver_incremental/"
    f"batch_id={bronze_batch_id}/"
    f"silver_run_id={silver_run_id}/"
    "validation_results.json"
)

overall_status = (
    "VALIDATED"
    if failed_count == 0 and passed_count == 48
    else "FAILED"
)

validation_manifest = {
    "BronzeBatchID": bronze_batch_id,
    "BronzeAttemptID": attempt_id,
    "SilverRunID": silver_run_id,
    "Status": overall_status,
    "ExpectedTableCount": 48,
    "PassedTableCount": passed_count,
    "FailedTableCount": failed_count,
    "ValidatedAtUTC": datetime.now(
        timezone.utc
    ).isoformat(),
    "Tables": validation_results
}

notebookutils.fs.put(
    validation_manifest_path,
    json.dumps(
        validation_manifest,
        indent=2,
        default=str
    ),
    True
)

print("Validation manifest:", validation_manifest_path)
print("Overall status:", overall_status)

if overall_status != "VALIDATED":
    raise RuntimeError(
        f"Silver validation failed: "
        f"{failed_count} of 48 tables failed"
    )

exit_payload = {
    "status": "VALIDATED",
    "bronze_batch_id": bronze_batch_id,
    "silver_run_id": silver_run_id,
    "passed_table_count": passed_count,
    "failed_table_count": failed_count,
    "validation_manifest_path": validation_manifest_path
}

notebookutils.notebook.exit(
    json.dumps(exit_payload)
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
