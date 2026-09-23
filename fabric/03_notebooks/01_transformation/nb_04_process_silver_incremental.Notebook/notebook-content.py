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

BRONZE_ROOT = (
    "abfss://c64ac1e1-7c4e-48d9-a80e-89d584a3b1c0"
    "@onelake.dfs.fabric.microsoft.com/"
    "db08a2e9-f6c8-4a56-8540-94277a004073"
)

manifest_root = (
    f"{BRONZE_ROOT}/Files/manifests/wwi/bronze_incremental/"
    f"batch_id={bronze_batch_id}/"
    f"attempt_id={attempt_id}"
)

success_path = f"{manifest_root}/_SUCCESS.json"


def read_complete_json(path):
    binary_row = (
        spark.read
        .format("binaryFile")
        .load(path)
        .select("content")
        .first()
    )

    if binary_row is None:
        raise RuntimeError(f"Could not read JSON file: {path}")

    json_text = bytes(binary_row["content"]).decode("utf-8-sig").strip()

    if not json_text:
        raise RuntimeError(f"JSON file is empty: {path}")

    return json.loads(json_text)


if not bronze_batch_id.strip():
    raise ValueError("bronze_batch_id is required")

if not attempt_id.strip():
    raise ValueError("attempt_id is required")

if not notebookutils.fs.exists(success_path):
    raise RuntimeError(
        f"Bronze batch is not approved. Missing: {success_path}"
    )

root_items = notebookutils.fs.ls(manifest_root)

success_items = [
    item for item in root_items
    if item.name == "_SUCCESS.json"
]

if not success_items:
    raise RuntimeError("Bronze _SUCCESS.json was not found")

success_size = success_items[0].size

if success_size <= 0:
    raise RuntimeError("Bronze _SUCCESS.json is empty")

print("Bronze batch approved")
print("Batch ID:", bronze_batch_id)
print("Attempt ID:", attempt_id)
print("SUCCESS marker size:", success_size, "bytes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def find_json_receipts(folder_path):
    receipt_paths = []

    for item in notebookutils.fs.ls(folder_path):
        if item.isDir:
            receipt_paths.extend(find_json_receipts(item.path))
        elif item.path.endswith(".json") and not item.path.endswith("_SUCCESS.json"):
            receipt_paths.append(item.path)

    return receipt_paths


receipt_paths = sorted(find_json_receipts(manifest_root))

print("Receipt count:", len(receipt_paths))

if len(receipt_paths) != 48:
    raise RuntimeError(
        f"Expected 48 Bronze table receipts, found {len(receipt_paths)}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_receipt_text = notebookutils.fs.head(
    receipt_paths[0],
    1024 * 1024
).strip()

sample_receipt = json.loads(sample_receipt_text)

print("Sample receipt:", receipt_paths[0])
print("Receipt fields:", list(sample_receipt.keys()))

display(sample_receipt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, StringType,
    BooleanType, LongType
)

receipt_records = []
receipt_errors = []

for receipt_path in receipt_paths:
    receipt = read_complete_json(receipt_path)

    identity = (
        f"{receipt.get('SourceSchema')}."
        f"{receipt.get('SourceTable')}"
    )

    if receipt.get("BatchID") != bronze_batch_id:
        receipt_errors.append(f"{identity}: BatchID mismatch")

    if str(receipt.get("AttemptID")) != attempt_id:
        receipt_errors.append(f"{identity}: AttemptID mismatch")

    if receipt.get("BatchStatus") != "VALIDATED":
        receipt_errors.append(
            f"{identity}: status is {receipt.get('BatchStatus')}"
        )

    expected = int(receipt.get("SourceRowCount", 0))
    rows_read = int(receipt.get("CopyRowsRead", 0))
    rows_copied = int(receipt.get("CopyRowsCopied", 0))
    bronze_count = int(receipt.get("BronzeRowCount", 0))

    if len({expected, rows_read, rows_copied, bronze_count}) != 1:
        receipt_errors.append(f"{identity}: row counts do not match")

    receipt_records.append((
        receipt["SourceSchema"],
        receipt["SourceTable"],
        receipt["TableType"],
        receipt["ExtractionMethod"],
        receipt["LoadType"],
        receipt.get("WatermarkColumn"),
        receipt.get("LowerWatermark"),
        receipt.get("UpperWatermark"),
        bool(receipt.get("AdvanceWatermark")),
        expected,
        bronze_count,
        receipt["RawPath"]
    ))


if receipt_errors:
    raise RuntimeError(
        "Receipt validation failed:\n" + "\n".join(receipt_errors)
    )

unique_tables = {
    (row[0], row[1])
    for row in receipt_records
}

if len(unique_tables) != 48:
    raise RuntimeError(
        f"Expected 48 unique tables, found {len(unique_tables)}"
    )


receipt_schema = StructType([
    StructField("SourceSchema", StringType(), False),
    StructField("SourceTable", StringType(), False),
    StructField("TableType", StringType(), True),
    StructField("ExtractionMethod", StringType(), False),
    StructField("LoadType", StringType(), False),
    StructField("WatermarkColumn", StringType(), True),
    StructField("LowerWatermark", StringType(), True),
    StructField("UpperWatermark", StringType(), True),
    StructField("AdvanceWatermark", BooleanType(), False),
    StructField("SourceRowCount", LongType(), False),
    StructField("BronzeRowCount", LongType(), False),
    StructField("RawPath", StringType(), False)
])

receipts_df = spark.createDataFrame(
    receipt_records,
    receipt_schema
)

print("Validated receipts:", receipts_df.count())

display(
    receipts_df
    .groupBy("ExtractionMethod")
    .count()
    .orderBy("ExtractionMethod")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

config_df = (
    spark.table("wwi_bronze.ops.table_config")
    .filter(F.col("IsActive") == True)
    .select(
        "ConfigID",
        "SourceSchema",
        "SourceTable",
        "PrimaryKeyColumns",
        "SilverSchema",
        "SilverTable",
        "ConfigStatus"
    )
)

processing_plan_df = (
    receipts_df.alias("receipt")
    .join(
        config_df.alias("config"),
        on=["SourceSchema", "SourceTable"],
        how="left"
    )
)

missing_config_df = processing_plan_df.filter(
    F.col("ConfigID").isNull()
)

missing_config_count = missing_config_df.count()

if missing_config_count > 0:
    display(missing_config_df)

    raise RuntimeError(
        f"{missing_config_count} Bronze tables have no active configuration"
    )

if processing_plan_df.count() != 48:
    raise RuntimeError(
        "Processing plan must contain exactly 48 tables"
    )

print("Silver processing plan ready:", processing_plan_df.count())

display(
    processing_plan_df.select(
        "SourceSchema",
        "SourceTable",
        "ExtractionMethod",
        "BronzeRowCount",
        "PrimaryKeyColumns",
        "SilverSchema",
        "SilverTable"
    ).orderBy(
        "SourceSchema",
        "SourceTable"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from pyspark.sql import Row


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


compatibility_results = []

for spec in processing_plan_df.collect():
    source_schema = spec["SourceSchema"]
    source_table = spec["SourceTable"]
    silver_schema = spec["SilverSchema"]
    silver_table = spec["SilverTable"]

    raw_path = f"{BRONZE_ROOT}/{spec['RawPath']}"

    silver_target = (
        f"wwi_silver.{silver_schema}.{silver_table}"
    )

    try:
        bronze_df = spark.read.parquet(raw_path)

        normalized_bronze_columns = {
            to_snake_case(column)
            for column in bronze_df.columns
        }

        silver_df = spark.table(silver_target)

        silver_business_columns = {
            column
            for column in silver_df.columns
            if not column.startswith("_")
        }

        missing_in_bronze = sorted(
            silver_business_columns -
            normalized_bronze_columns
        )

        extra_in_bronze = sorted(
            normalized_bronze_columns -
            silver_business_columns
        )

        compatibility_results.append(Row(
            SourceSchema=source_schema,
            SourceTable=source_table,
            SilverTarget=silver_target,
            TargetExists=True,
            MissingInBronze=", ".join(missing_in_bronze),
            ExtraInBronze=", ".join(extra_in_bronze),
            Error=""
        ))

    except Exception as error:
        compatibility_results.append(Row(
            SourceSchema=source_schema,
            SourceTable=source_table,
            SilverTarget=silver_target,
            TargetExists=False,
            MissingInBronze="",
            ExtraInBronze="",
            Error=str(error)[:500]
        ))


compatibility_df = spark.createDataFrame(
    compatibility_results
)

missing_targets = compatibility_df.filter(
    F.col("TargetExists") == False
).count()

print("Tables checked:", compatibility_df.count())
print("Missing or unreadable targets:", missing_targets)

display(
    compatibility_df.orderBy(
        F.desc("TargetExists"),
        "SourceSchema",
        "SourceTable"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType


def normalize_dataframe_columns(df):
    renamed_df = df

    for original_name in df.columns:
        normalized_name = to_snake_case(original_name)

        if original_name != normalized_name:
            renamed_df = renamed_df.withColumnRenamed(
                original_name,
                normalized_name
            )

    return renamed_df


def build_silver_dataframe(spec):
    raw_path = f"{BRONZE_ROOT}/{spec['RawPath']}"

    silver_target = (
        f"wwi_silver."
        f"{spec['SilverSchema']}."
        f"{spec['SilverTable']}"
    )

    bronze_df = spark.read.parquet(raw_path)
    bronze_df = normalize_dataframe_columns(bronze_df)

    target_schema = spark.table(silver_target).schema
    source_columns = set(bronze_df.columns)

    metadata_expressions = {
    "_batch_id": F.lit(bronze_batch_id),
    "_source_batch_id": F.lit(bronze_batch_id),
    "_source_attempt_id": F.lit(attempt_id),
    "_silver_run_id": F.lit(silver_run_id),
    "_load_type": F.lit(spec["LoadType"]),
    "_source_schema": F.lit(spec["SourceSchema"]),
    "_source_table": F.lit(spec["SourceTable"]),
    "_source_path": F.lit(spec["RawPath"]),
    "_ingestion_date_utc": F.to_timestamp(
        F.lit(spec["IngestionDateUTC"])
    ) if "IngestionDateUTC" in spec.asDict() else F.current_timestamp(),
    "_processed_at_utc": F.current_timestamp(),
    "_loaded_at_utc": F.current_timestamp()
}

    selected_columns = []
    unresolved_columns = []

    for target_field in target_schema.fields:
        target_column = target_field.name
        target_type = target_field.dataType

        # Direct Bronze-to-Silver column
        if target_column in source_columns:
            selected_columns.append(
                F.col(target_column)
                .cast(target_type)
                .alias(target_column)
            )

        # Preserve exact source datetime text where required
        elif target_column.endswith("_source_text"):
            base_column = target_column.removesuffix("_source_text")

            if base_column in source_columns:
                selected_columns.append(
                    F.col(base_column)
                    .cast("string")
                    .alias(target_column)
                )
            else:
                unresolved_columns.append(target_column)

        # Pipeline metadata column
        elif target_column in metadata_expressions:
            selected_columns.append(
                metadata_expressions[target_column]
                .cast(target_type)
                .alias(target_column)
            )

        else:
            unresolved_columns.append(target_column)

    if unresolved_columns:
        raise RuntimeError(
            f"{silver_target}: no transformation defined for "
            f"{unresolved_columns}"
        )

    transformed_df = bronze_df.select(*selected_columns)

    return transformed_df, silver_target

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_spec = (
    processing_plan_df
    .filter(
        (F.col("SourceSchema") == "Sales") &
        (F.col("SourceTable") == "Orders")
    )
    .first()
)

if orders_spec is None:
    raise RuntimeError("Sales.Orders was not found in the processing plan")

orders_staged_df, orders_target = build_silver_dataframe(
    orders_spec
)

orders_staged_count = orders_staged_df.count()
orders_expected_count = int(orders_spec["BronzeRowCount"])

print("Silver target:", orders_target)
print("Bronze expected rows:", orders_expected_count)
print("Transformed rows:", orders_staged_count)

if orders_staged_count != orders_expected_count:
    raise RuntimeError(
        "Sales.Orders transformation changed the row count"
    )

orders_staged_df.printSchema()

display(
    orders_staged_df.limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import re


def parse_primary_keys(primary_key_value):
    if primary_key_value is None:
        return []

    if isinstance(primary_key_value, (list, tuple)):
        values = list(primary_key_value)
    else:
        text = str(primary_key_value).strip()

        if not text:
            return []

        # Handle JSON arrays such as ["OrderID", "OrderLineID"]
        try:
            parsed = json.loads(text)

            if isinstance(parsed, list):
                values = parsed
            else:
                values = [parsed]

        except Exception:
            # Handle SQL-style values such as [OrderID], [OrderLineID]
            bracket_values = re.findall(r"\[([^\]]+)\]", text)

            if bracket_values:
                values = bracket_values
            else:
                values = text.split(",")

    normalized_keys = []

    for value in values:
        key = to_snake_case(
            str(value).strip().strip("'\"[]")
        )

        if key and key not in normalized_keys:
            normalized_keys.append(key)

    return normalized_keys


def get_base_table_keys(source_schema, archive_table):
    base_table = archive_table.removesuffix("_Archive")

    base_config = (
        config_df
        .filter(
            (F.col("SourceSchema") == source_schema) &
            (F.col("SourceTable") == base_table)
        )
        .select("PrimaryKeyColumns")
        .first()
    )

    if base_config is None:
        return []

    return parse_primary_keys(
        base_config["PrimaryKeyColumns"]
    )


def resolve_operation_keys(spec, staged_columns):
    method = spec["ExtractionMethod"]

    if method == "SNAPSHOT":
        return []

    keys = parse_primary_keys(
        spec["PrimaryKeyColumns"]
    )

    if method == "TEMPORAL_HISTORY_APPEND":
        if not keys:
            keys = get_base_table_keys(
                spec["SourceSchema"],
                spec["SourceTable"]
            )

        temporal_version_keys = []

        if "valid_from_source_text" in staged_columns:
            temporal_version_keys.append(
                "valid_from_source_text"
            )
        elif "valid_from" in staged_columns:
            temporal_version_keys.append(
                "valid_from"
            )
        else:
            raise RuntimeError(
                f"{spec['SourceSchema']}."
                f"{spec['SourceTable']}: "
                "ValidFrom version column is missing"
            )

        if "valid_to_source_text" in staged_columns:
            temporal_version_keys.append(
                "valid_to_source_text"
            )
        elif "valid_to" in staged_columns:
            temporal_version_keys.append(
                "valid_to"
            )
        else:
            raise RuntimeError(
                f"{spec['SourceSchema']}."
                f"{spec['SourceTable']}: "
                "ValidTo version column is missing"
            )

        for version_key in temporal_version_keys:
            if version_key not in keys:
                keys.append(version_key)

    if not keys:
        raise RuntimeError(
            f"{spec['SourceSchema']}."
            f"{spec['SourceTable']}: "
            f"no keys configured for {method}"
        )

    missing_keys = [
        key for key in keys
        if key not in staged_columns
    ]

    if missing_keys:
        raise RuntimeError(
            f"{spec['SourceSchema']}."
            f"{spec['SourceTable']}: "
            f"merge keys missing from input: {missing_keys}"
        )

    return keys

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

operation_plan = []
preflight_errors = []

for spec in processing_plan_df.collect():
    identity = (
        f"{spec['SourceSchema']}."
        f"{spec['SourceTable']}"
    )

    try:
        staged_df, silver_target = build_silver_dataframe(spec)

        merge_keys = resolve_operation_keys(
            spec,
            staged_df.columns
        )

        operation_plan.append({
            "SourceSchema": spec["SourceSchema"],
            "SourceTable": spec["SourceTable"],
            "ExtractionMethod": spec["ExtractionMethod"],
            "SilverTarget": silver_target,
            "MergeKeys": ", ".join(merge_keys),
            "BronzeRowCount": int(spec["BronzeRowCount"])
        })

    except Exception as error:
        preflight_errors.append({
            "Table": identity,
            "Error": str(error)
        })


if preflight_errors:
    display(spark.createDataFrame(preflight_errors))

    raise RuntimeError(
        f"Silver preflight failed for "
        f"{len(preflight_errors)} tables"
    )

operation_plan_df = spark.createDataFrame(operation_plan)

print("Tables ready for Silver processing:",
      operation_plan_df.count())

display(
    operation_plan_df.orderBy(
        "ExtractionMethod",
        "SourceSchema",
        "SourceTable"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.storagelevel import StorageLevel


def build_merge_condition(keys):
    return " AND ".join(
        f"target.`{key}` <=> source.`{key}`"
        for key in keys
    )


def verify_unique_keys(df, keys, identity):
    duplicate_exists = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_exists:
        raise RuntimeError(
            f"{identity}: duplicate source merge keys found: {keys}"
        )


def apply_silver_operation(spec):
    identity = (
        f"{spec['SourceSchema']}."
        f"{spec['SourceTable']}"
    )

    method = spec["ExtractionMethod"]

    staged_df, silver_target = build_silver_dataframe(spec)

    staged_df = staged_df.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    try:
        source_count = staged_df.count()
        expected_count = int(spec["BronzeRowCount"])

        if source_count != expected_count:
            raise RuntimeError(
                f"{identity}: transformed count {source_count} "
                f"does not match Bronze count {expected_count}"
            )

        merge_keys = resolve_operation_keys(
            spec,
            staged_df.columns
        )

        # A zero-row incremental batch is a valid no-op.
        if source_count == 0 and method != "SNAPSHOT":
            return {
                "SourceSchema": spec["SourceSchema"],
                "SourceTable": spec["SourceTable"],
                "SilverTarget": silver_target,
                "Method": mfrom delta.tables import DeltaTable
from pyspark.storagelevel import StorageLevel


def with_delta_state(result, silver_target):
    latest = (
        DeltaTable.forName(spark, silver_target)
        .history(1)
        .select("version", "timestamp")
        .first()
    )
    if latest is None:
        raise RuntimeError(
            f"{silver_target}: Delta history is unavailable after processing"
        )

    return {
        **result,
        "DeltaVersionAfterProcessing": int(latest["version"]),
        "DeltaTimestampAfterProcessing": latest["timestamp"].isoformat()
    }


def build_merge_condition(keys):
    return " AND ".join(
        "target.`{}` <=> source.`{}`".format(key, key)
        for key in keys
    )


def verify_unique_keys(df, keys, identity):
    duplicate_exists = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_exists:
        raise RuntimeError(
            f"{identity}: duplicate source merge keys found: {keys}"
        )


def apply_silver_operation(spec):
    identity = (
        f"{spec['SourceSchema']}."
        f"{spec['SourceTable']}"
    )

    method = spec["ExtractionMethod"]
    staged_df, silver_target = build_silver_dataframe(spec)
    staged_df = staged_df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        source_count = staged_df.count()
        expected_count = int(spec["BronzeRowCount"])

        if source_count != expected_count:
            raise RuntimeError(
                f"{identity}: transformed count {source_count} "
                f"does not match Bronze count {expected_count}"
            )

        merge_keys = resolve_operation_keys(spec, staged_df.columns)

        if source_count == 0 and method != "SNAPSHOT":
            return with_delta_state({
                "SourceSchema": spec["SourceSchema"],
                "SourceTable": spec["SourceTable"],
                "SilverTarget": silver_target,
                "Method": method,
                "MergeKeys": ", ".join(merge_keys),
                "SourceRows": source_count,
                "Result": "NO_CHANGES"
            }, silver_target)

        if method == "SNAPSHOT":
            staged_df.write                 .format("delta")                 .mode("overwrite")                 .option("overwriteSchema", "false")                 .saveAsTable(silver_target)
            result = "SNAPSHOT_REPLACED"

        elif method in {"WATERMARK_UPSERT", "TEMPORAL_CURRENT_UPSERT"}:
            verify_unique_keys(staged_df, merge_keys, identity)
            merge_condition = build_merge_condition(merge_keys)
            (
                DeltaTable.forName(spark, silver_target)
                .alias("target")
                .merge(staged_df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            result = "MERGED"

        elif method in {"WATERMARK_APPEND", "TEMPORAL_HISTORY_APPEND"}:
            verify_unique_keys(staged_df, merge_keys, identity)
            merge_condition = build_merge_condition(merge_keys)
            (
                DeltaTable.forName(spark, silver_target)
                .alias("target")
                .merge(staged_df.alias("source"), merge_condition)
                .whenNotMatchedInsertAll()
                .execute()
            )
            result = "APPENDED_IDEMPOTENTLY"

        else:
            raise RuntimeError(f"{identity}: unsupported method {method}")

        return with_delta_state({
            "SourceSchema": spec["SourceSchema"],
            "SourceTable": spec["SourceTable"],
            "SilverTarget": silver_target,
            "Method": method,
            "MergeKeys": ", ".join(merge_keys),
            "SourceRows": source_count,
            "Result": result
        }, silver_target)

    finally:
        staged_df.unpersist()
e"),
                    merge_condition
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            result = "MERGED"

        elif method in {
            "WATERMARK_APPEND",
            "TEMPORAL_HISTORY_APPEND"
        }:
            verify_unique_keys(
                staged_df,
                merge_keys,
                identity
            )

            merge_condition = build_merge_condition(
                merge_keys
            )

            (
                DeltaTable.forName(spark, silver_target)
                .alias("target")
                .merge(
                    staged_df.alias("source"),
                    merge_condition
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

            result = "APPENDED_IDEMPOTENTLY"

        else:
            raise RuntimeError(
                f"{identity}: unsupported method {method}"
            )

        return {
            "SourceSchema": spec["SourceSchema"],
            "SourceTable": spec["SourceTable"],
            "SilverTarget": silver_target,
            "Method": method,
            "MergeKeys": ", ".join(merge_keys),
            "SourceRows": source_count,
            "Result": result
        }

    finally:
        staged_df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_results = []
silver_failures = []

ordered_specs = (
    processing_plan_df
    .orderBy("SourceSchema", "SourceTable")
    .collect()
)

for position, spec in enumerate(ordered_specs, start=1):
    identity = (
        f"{spec['SourceSchema']}."
        f"{spec['SourceTable']}"
    )

    print(
        f"[{position}/48] Processing {identity} "
        f"using {spec['ExtractionMethod']}"
    )

    try:
        result = apply_silver_operation(spec)
        silver_results.append(result)

        print("   Result:", result["Result"])

    except Exception as error:
        silver_failures.append({
            "SourceSchema": spec["SourceSchema"],
            "SourceTable": spec["SourceTable"],
            "Error": str(error)
        })

        print("   FAILED:", str(error))
        break


if silver_results:
    silver_results_df = spark.createDataFrame(
        silver_results
    )

    display(
        silver_results_df.orderBy(
            "SourceSchema",
            "SourceTable"
        )
    )

if silver_failures:
    display(spark.createDataFrame(silver_failures))

    raise RuntimeError(
        f"Silver processing stopped after "
        f"{len(silver_results)} successful tables"
    )

if len(silver_results) != 48:
    raise RuntimeError(
        f"Expected 48 processed tables, "
        f"completed {len(silver_results)}"
    )

print("SILVER PROCESSING COMPLETED: 48 tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone
import json

silver_manifest_directory = (
    "Files/manifests/wwi/silver_incremental/"
    f"batch_id={bronze_batch_id}/"
    f"silver_run_id={silver_run_id}"
)

process_manifest_path = (
    f"{silver_manifest_directory}/"
    "process_results.json"
)

process_manifest = {
    "BronzeBatchID": bronze_batch_id,
    "BronzeAttemptID": attempt_id,
    "SilverRunID": silver_run_id,
    "Status": "PROCESSED",
    "ProcessedTableCount": len(silver_results),
    "CompletedAtUTC": datetime.now(timezone.utc).isoformat(),
    "Tables": silver_results
}

notebookutils.fs.mkdirs(
    silver_manifest_directory
)

notebookutils.fs.put(
    process_manifest_path,
    json.dumps(
        process_manifest,
        indent=2,
        default=str
    ),
    True
)

if not notebookutils.fs.exists(process_manifest_path):
    raise RuntimeError(
        "Silver process manifest was not written"
    )

print("Silver processing manifest written")
print("Path:", process_manifest_path)
print("Processed tables:", len(silver_results))

exit_payload = {
    "status": "PROCESSED",
    "bronze_batch_id": bronze_batch_id,
    "attempt_id": attempt_id,
    "silver_run_id": silver_run_id,
    "processed_table_count": len(silver_results),
    "process_manifest_path": process_manifest_path
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
