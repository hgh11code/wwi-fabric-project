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
# META         },
# META         {
# META           "id": "db08a2e9-f6c8-4a56-8540-94277a004073"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # WWI Bronze to Silver — full load
# 
# Cleaned replacement for `nb_01_bronze_to_silver_full`. Attach `wwi_silver` as default and `wwi_bronze` additionally. Run all cells in order in a fresh session. Requires Spark 3.5+.
# 
# **Full loads only:** this notebook overwrites the configured 48 managed Silver tables. Never supply an incremental batch. Bronze files and Bronze operational tables are read-only. No source ingestion or operational reinitialization is performed.
# 
# Pipeline: mark Cell 1 as the parameter cell if the import loses its tag. Pass `p_batch_id` from the validated Bronze master batch. An empty value selects the latest complete validated FULL batch; tied timestamps stop. Set `p_pipeline_exit=True` for a pipeline return value.
# 
# Run only one Silver writer at a time. Writes are atomic per Delta table, not across 48 tables. Downstream Gold must wait for successful notebook completion. A retry starts a new Silver RunID and repeats the approved full load; previous quality records remain.
# 
# Configured destination names are preserved, including existing double-underscore archive names. Renaming them requires a separate metadata/table migration. History validation uses parent keys plus original ValidFrom and ValidTo text; uniqueness is checked on every run.
# 
# This is a cleaned full-load implementation, not an incremental implementation. The datetime contract is name-based for WWI; schema metadata does not currently provide original SQL types. Unknown string columns ending in `_when`, `_time`, or starting `valid_` stop for review. Expand the explicit contract for future source schema changes.
# 
# Validation performed locally: Python syntax, notebook format, pure-Python metadata/key tests. Fabric/OneLake/Delta execution must be verified in your workspace before scheduling.


# MARKDOWN ********************

# **Cell 1 — Parameters**
# 
# Expected: no output. Pipeline supplies the approved Bronze BatchID.

# PARAMETERS CELL ********************

p_batch_id = ""
p_pipeline_exit = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 2 — Initialize one execution**
# 
# Expected: a fresh RunID. UTC is explicit; no fixed row-total baseline.

# CELL ********************

from pyspark.sql import functions as F
from pyspark import StorageLevel
from delta.tables import DeltaTable
from datetime import datetime, timezone
import uuid, json, re
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.timestampType", "TIMESTAMP_LTZ")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
SILVER_RUN_ID = str(uuid.uuid4())
PROCESSED_AT_UTC = datetime.now(timezone.utc)
BRONZE_LAKEHOUSE = "wwi_bronze"
EXPECTED_TABLE_COUNT = 48
BRONZE_ABFSS_ROOT = "abfss://wwi_analytics@onelake.dfs.fabric.microsoft.com/wwi_bronze.Lakehouse"
APPROVED_BATCH_ID = str(p_batch_id or "").strip()
APPROVED_BRONZE_ROW_COUNT = 0
active_config_count = 48
print("Silver RunID:", SILVER_RUN_ID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 3 — Quality storage and low-level helpers**
# 
# Creates missing Silver quality tables only; preserves existing logs. Failure to access logging storage raises immediately.

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS wwi_silver.quality.silver_run_log (
    silver_run_id STRING,
    batch_id STRING,
    load_type STRING,
    run_status STRING,
    expected_table_count INT,
    completed_table_count INT,
    failed_table_count INT,
    expected_bronze_row_count BIGINT,
    processed_silver_row_count BIGINT,
    started_at_utc TIMESTAMP,
    completed_at_utc TIMESTAMP,
    error_message STRING
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS wwi_silver.quality.silver_table_results (
    silver_run_id STRING,
    batch_id STRING,
    source_schema STRING,
    source_table STRING,
    silver_schema STRING,
    silver_table STRING,
    table_status STRING,
    expected_row_count BIGINT,
    actual_row_count BIGINT,
    row_count_difference BIGINT,
    null_primary_key_rows BIGINT,
    duplicate_key_rows BIGINT,
    invalid_timestamp_values BIGINT,
    bronze_column_count INT,
    silver_column_count INT,
    completed_at_utc TIMESTAMP,
    error_message STRING
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS wwi_silver.quality.silver_validation_results (
    silver_run_id STRING,
    batch_id STRING,
    source_schema STRING,
    source_table STRING,
    check_name STRING,
    expected_value STRING,
    actual_value STRING,
    check_status STRING,
    checked_at_utc TIMESTAMP
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS wwi_silver.quality.silver_failures (
    silver_run_id STRING,
    batch_id STRING,
    source_schema STRING,
    source_table STRING,
    processing_stage STRING,
    error_type STRING,
    error_message STRING,
    failed_at_utc TIMESTAMP
)
USING DELTA
""")

print("Silver quality tables are ready.")
from delta.tables import DeltaTable
from datetime import datetime, timezone

RUN_LOG_TABLE = "wwi_silver.quality.silver_run_log"
TABLE_RESULTS_TABLE = (
    "wwi_silver.quality.silver_table_results"
)
VALIDATION_RESULTS_TABLE = (
    "wwi_silver.quality.silver_validation_results"
)
FAILURES_TABLE = "wwi_silver.quality.silver_failures"


def current_utc_timestamp():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def dataframe_from_target_schema(table_name, records):
    target_schema = spark.table(table_name).schema

    return spark.createDataFrame(
        records,
        schema=target_schema
    )


def merge_quality_records(
    target_table,
    source_df,
    merge_condition
):
    target_delta = DeltaTable.forName(
        spark,
        target_table
    )

    (
        target_delta.alias("target")
        .merge(
            source_df.alias("source"),
            merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 4 — Names, keys and timestamps**
# 
# Definitions only. Preserve exact datetime2 text; reject invalid non-null timestamps, including blanks.

# CELL ********************

def to_snake_case(name):
    value = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    return re.sub(r"_+", "_", re.sub(r"[^A-Za-z0-9_]+", "_", value)).lower().strip("_")

def parse_primary_key_columns(value):
    if not value or not value.strip(): return []
    try:
        keys = json.loads(value)
        if not isinstance(keys, list): keys = [keys]
    except (ValueError, TypeError):
        keys = re.split(r"[,;|]", value)
    if not all(isinstance(k, str) and k.strip() for k in keys):
        raise ValueError(f"Invalid primary key configuration: {value!r}")
    return list(dict.fromkeys(to_snake_case(k.strip().strip("[]\"'")) for k in keys))

WWI_DATETIME2_COLUMNS = {
    "valid_from", "valid_to", "last_edited_when", "recorded_when",
    "transaction_occurred_when", "picking_completed_when",
    "confirmed_delivery_time", "returned_delivery_data_received_when"
}
DATETIME2_REGEX = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?$"

def parse_wwi_datetime2(column_name):
    value = F.col(column_name)
    return F.when(value.rlike(DATETIME2_REGEX), F.try_to_timestamp(
        value, F.lit("yyyy-MM-dd'T'HH:mm:ss[.SSSSSSS]")))

def get_silver_validation_keys(record):
    history = record["TableType"] == "HISTORY_TABLE"
    config = CONFIG_LOOKUP[(record["SourceSchema"], record["SourceTable"])]
    if history:
        parent = re.sub(r"_archive$", "", record["SourceTable"], flags=re.I)
        config = CONFIG_LOOKUP.get((record["SourceSchema"], parent))
        if config is None: raise ValueError("History parent not configured")
    keys = parse_primary_key_columns(config["PrimaryKeyColumns"])
    if not keys: raise ValueError(f"Missing business key: {record['SourceTable']}")
    if history: keys += ["valid_from_source_text", "valid_to_source_text"]
    return list(dict.fromkeys(keys))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 5 — Source-aligned transformation**
# 
# Definitions only. No joins, row drops or binary decoding.

# CELL ********************

def transform_bronze_to_silver(
    bronze_df,
    source_schema,
    source_table,
    batch_id,
    attempt_id,
    silver_run_id,
    processed_at_utc
):
    """
    Apply standard Silver transformations without writing the table.
    """

    types = {to_snake_case(f.name): f.dataType.simpleString() for f in bronze_df.schema.fields}
    unknown = [name for name, dtype in types.items() if dtype == "string"
               and (name.endswith(("_when", "_time")) or name.startswith("valid_"))
               and name not in WWI_DATETIME2_COLUMNS]
    if unknown: raise ValueError(f"Review unconfigured datetime columns: {unknown}")
    for name in WWI_DATETIME2_COLUMNS.intersection(types):
        if types[name] != "string": raise TypeError(f"Expected Bronze datetime2 string: {name}")
        if name + "_source_text" in types: raise ValueError("Source text column collision")
    if any(name.startswith("_") for name in bronze_df.columns):
        raise ValueError("Source column conflicts with reserved technical prefix")
    transformed_df = bronze_df

    # ---------------------------------------------------------
    # 1. Convert business column names to snake_case
    # ---------------------------------------------------------
    renamed_columns = [
        to_snake_case(column_name)
        for column_name in bronze_df.columns
    ]

    if len(renamed_columns) != len(set(renamed_columns)):
        raise ValueError(
            f"Column-name collision after snake_case conversion for "
            f"{source_schema}.{source_table}."
        )

    for original_name, new_name in zip(
        bronze_df.columns,
        renamed_columns
    ):
        if original_name != new_name:
            transformed_df = transformed_df.withColumnRenamed(
                original_name,
                new_name
            )

    # ---------------------------------------------------------
    # 2. Locate datetime2 columns present in this table
    # ---------------------------------------------------------
    datetime_columns = [
        column_name
        for column_name in transformed_df.columns
        if column_name in WWI_DATETIME2_COLUMNS
    ]

    # ---------------------------------------------------------
    # 3. Preserve exact source text and create typed timestamp
    # ---------------------------------------------------------
    for column_name in datetime_columns:
        source_text_column = f"{column_name}_source_text"

        transformed_df = (
            transformed_df
            .withColumn(
                source_text_column,
                F.col(column_name)
            )
            .withColumn(
                column_name,
                parse_wwi_datetime2(column_name)
            )
        )

    # ---------------------------------------------------------
    # 4. Add Silver technical metadata
    # ---------------------------------------------------------
    transformed_df = (
        transformed_df
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_load_type", F.lit("FULL"))
        .withColumn("_source_schema", F.lit(source_schema))
        .withColumn("_source_table", F.lit(source_table))
        .withColumn("_source_attempt_id", F.lit(attempt_id))
        .withColumn("_silver_run_id", F.lit(silver_run_id))
        .withColumn(
            "_processed_at_utc",
            F.lit(processed_at_utc).cast("timestamp")
        )
    )

    return transformed_df, datetime_columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 6 — Quality gate**
# 
# Definitions only. Checks keys, counts, metadata and timestamp conversion; each table must pass before writing.

# CELL ********************

def validate_silver_dataframe(
    silver_df,
    expected_row_count,
    expected_bronze_column_count,
    primary_key_columns,
    datetime_columns
):
    validation = {}

    actual_row_count = silver_df.count()
    actual_silver_column_count = len(silver_df.columns)

    validation["expected_row_count"] = int(expected_row_count)
    validation["actual_row_count"] = actual_row_count
    validation["row_count_difference"] = (
        actual_row_count - int(expected_row_count)
    )

    # Validate that every configured primary-key column exists
    missing_key_columns = [
        column_name
        for column_name in primary_key_columns
        if column_name not in silver_df.columns
    ]

    validation["missing_key_columns"] = missing_key_columns

    if missing_key_columns:
        validation["null_primary_key_rows"] = None
        validation["duplicate_key_rows"] = None

    elif not primary_key_columns:
        validation["null_primary_key_rows"] = None
        validation["duplicate_key_rows"] = None

    else:
        # A composite key is invalid if any part is NULL
        null_key_condition = None

        for column_name in primary_key_columns:
            column_condition = F.col(column_name).isNull()

            if null_key_condition is None:
                null_key_condition = column_condition
            else:
                null_key_condition = (
                    null_key_condition | column_condition
                )

        validation["null_primary_key_rows"] = (
            silver_df
            .filter(null_key_condition)
            .count()
        )

        duplicate_summary = (
            silver_df
            .groupBy(*primary_key_columns)
            .count()
            .filter(F.col("count") > 1)
            .agg(
                F.coalesce(
                    F.sum(F.col("count") - 1),
                    F.lit(0)
                ).alias("duplicate_rows")
            )
            .first()
        )

        validation["duplicate_key_rows"] = int(
            duplicate_summary["duplicate_rows"]
        )

    # Validate datetime conversions
    invalid_timestamp_count = 0

    for column_name in datetime_columns:
        source_text_column = f"{column_name}_source_text"

        current_invalid_count = (
            silver_df
            .filter(
                F.col(source_text_column).isNotNull()

                & F.col(column_name).isNull()
            )
            .count()
        )

        invalid_timestamp_count += current_invalid_count

    validation["invalid_timestamp_values"] = (
        invalid_timestamp_count
    )

    # Validate technical metadata
    technical_columns = [
        "_batch_id",
        "_load_type",
        "_source_schema",
        "_source_table",
        "_source_attempt_id",
        "_silver_run_id",
        "_processed_at_utc"
    ]

    missing_technical_columns = [
        column_name
        for column_name in technical_columns
        if column_name not in silver_df.columns
    ]

    validation["missing_technical_columns"] = (
        missing_technical_columns
    )

    if missing_technical_columns:
        validation["rows_with_missing_metadata"] = None
    else:
        missing_metadata_condition = None

        for column_name in technical_columns:
            column_condition = F.col(column_name).isNull()

            if missing_metadata_condition is None:
                missing_metadata_condition = column_condition
            else:
                missing_metadata_condition = (
                    missing_metadata_condition | column_condition
                )

        validation["rows_with_missing_metadata"] = (
            silver_df
            .filter(missing_metadata_condition)
            .count()
        )

    expected_silver_column_count = (
        int(expected_bronze_column_count)
        + len(datetime_columns)
        + len(technical_columns)
    )

    validation["expected_silver_column_count"] = (
        expected_silver_column_count
    )
    validation["actual_silver_column_count"] = (
        actual_silver_column_count
    )

    validation["passed"] = (
        validation["row_count_difference"] == 0
        and len(validation["missing_key_columns"]) == 0
        and len(primary_key_columns) > 0
        and validation["null_primary_key_rows"] == 0
        and validation["duplicate_key_rows"] == 0
        and validation["invalid_timestamp_values"] == 0
        and len(validation["missing_technical_columns"]) == 0
        and validation["rows_with_missing_metadata"] == 0
        and actual_silver_column_count
            == expected_silver_column_count
    )

    return validation

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 7 — Quality logging**
# 
# Definitions only. Results are keyed by the fresh Silver RunID.

# CELL ********************

def save_validation_results(
    manifest_record,
    validation
):
    checked_at = current_utc_timestamp()

    source_schema = manifest_record["SourceSchema"]
    source_table = manifest_record["SourceTable"]

    checks = [
        (
            "ROW_COUNT",
            str(validation["expected_row_count"]),
            str(validation["actual_row_count"]),
            validation["row_count_difference"] == 0
        ),
        (
            "PRIMARY_KEY_COLUMNS",
            "0 missing",
            str(len(validation["missing_key_columns"])),
            len(validation["missing_key_columns"]) == 0
        ),
        (
            "PRIMARY_KEY_NULL",
            "0",
            str(validation["null_primary_key_rows"]),
            validation["null_primary_key_rows"] == 0
        ),
        (
            "DUPLICATE_KEY",
            "0",
            str(validation["duplicate_key_rows"]),
            validation["duplicate_key_rows"] == 0
        ),
        (
            "DATETIME_CONVERSION",
            "0",
            str(validation["invalid_timestamp_values"]),
            validation["invalid_timestamp_values"] == 0
        ),
        (
            "TECHNICAL_METADATA",
            "0",
            str(validation["rows_with_missing_metadata"]),
            validation["rows_with_missing_metadata"] == 0
        ),
        (
            "SILVER_COLUMN_COUNT",
            str(validation["expected_silver_column_count"]),
            str(validation["actual_silver_column_count"]),
            (
                validation["expected_silver_column_count"]
                == validation["actual_silver_column_count"]
            )
        ),
        (
            "BINARY_TYPE_PRESERVATION",
            "0 changed",
            str(validation["binary_type_changes"]),
            validation["binary_type_changes"] == 0
        )
    ]

    records = []

    for check_name, expected, actual, passed in checks:
        records.append({
            "silver_run_id": SILVER_RUN_ID,
            "batch_id": APPROVED_BATCH_ID,
            "source_schema": source_schema,
            "source_table": source_table,
            "check_name": check_name,
            "expected_value": expected,
            "actual_value": actual,
            "check_status": (
                "PASSED" if passed else "FAILED"
            ),
            "checked_at_utc": checked_at
        })

    result_df = dataframe_from_target_schema(
        VALIDATION_RESULTS_TABLE,
        records
    )

    merge_quality_records(
        target_table=VALIDATION_RESULTS_TABLE,
        source_df=result_df,
        merge_condition="""
            target.silver_run_id = source.silver_run_id
            AND target.source_schema = source.source_schema
            AND target.source_table = source.source_table
            AND target.check_name = source.check_name
        """
    )


def save_table_result(
    manifest_record,
    validation,
    table_status,
    error_message=None
):
    actual_rows = (
        validation["actual_row_count"]
        if validation is not None
        else None
    )

    result_record = {
        "silver_run_id": SILVER_RUN_ID,
        "batch_id": APPROVED_BATCH_ID,
        "source_schema": manifest_record["SourceSchema"],
        "source_table": manifest_record["SourceTable"],
        "silver_schema": manifest_record["SilverSchema"],
        "silver_table": manifest_record["SilverTable"],
        "table_status": table_status,
        "expected_row_count": int(
            manifest_record["BronzeRowCount"]
        ),
        "actual_row_count": actual_rows,
        "row_count_difference": (
            validation["row_count_difference"]
            if validation is not None
            else None
        ),
        "null_primary_key_rows": (
            validation["null_primary_key_rows"]
            if validation is not None
            else None
        ),
        "duplicate_key_rows": (
            validation["duplicate_key_rows"]
            if validation is not None
            else None
        ),
        "invalid_timestamp_values": (
            validation["invalid_timestamp_values"]
            if validation is not None
            else None
        ),
        "bronze_column_count": int(
            manifest_record["BronzeColumnCount"]
        ),
        "silver_column_count": (
            validation["actual_silver_column_count"]
            if validation is not None
            else None
        ),
        "completed_at_utc": current_utc_timestamp(),
        "error_message": (
            str(error_message)[:4000]
            if error_message
            else None
        )
    }

    result_df = dataframe_from_target_schema(
        TABLE_RESULTS_TABLE,
        [result_record]
    )

    merge_quality_records(
        target_table=TABLE_RESULTS_TABLE,
        source_df=result_df,
        merge_condition="""
            target.silver_run_id = source.silver_run_id
            AND target.source_schema = source.source_schema
            AND target.source_table = source.source_table
        """
    )
def save_failure(
    manifest_record,
    processing_stage,
    error
):
    failure_record = {
        "silver_run_id": SILVER_RUN_ID,
        "batch_id": APPROVED_BATCH_ID,
        "source_schema": manifest_record["SourceSchema"],
        "source_table": manifest_record["SourceTable"],
        "processing_stage": processing_stage,
        "error_type": type(error).__name__,
        "error_message": str(error)[:4000],
        "failed_at_utc": current_utc_timestamp()
    }

    failure_df = dataframe_from_target_schema(
        FAILURES_TABLE,
        [failure_record]
    )

    merge_quality_records(
        target_table=FAILURES_TABLE,
        source_df=failure_df,
        merge_condition="""
            target.silver_run_id = source.silver_run_id
            AND target.source_schema = source.source_schema
            AND target.source_table = source.source_table
            AND target.processing_stage = source.processing_stage
        """
    )


def update_silver_run(
    run_status,
    error_message=None
):
    current_results_df = (
        spark.table(TABLE_RESULTS_TABLE)
        .filter(F.col("silver_run_id") == SILVER_RUN_ID)
    )

    completed_count = (
        current_results_df
        .filter(F.col("table_status") == "VALIDATED")
        .count()
    )

    failed_count = (
        current_results_df
        .filter(F.col("table_status") == "FAILED")
        .count()
    )

    processed_rows = (
        current_results_df
        .filter(F.col("table_status") == "VALIDATED")
        .agg(
            F.coalesce(
                F.sum("actual_row_count"),
                F.lit(0)
            ).alias("processed_rows")
        )
        .first()["processed_rows"]
    )

    updated_record = {
        "silver_run_id": SILVER_RUN_ID,
        "batch_id": APPROVED_BATCH_ID,
        "load_type": "FULL",
        "run_status": run_status,
        "expected_table_count": active_config_count,
        "completed_table_count": completed_count,
        "failed_table_count": failed_count,
        "expected_bronze_row_count": int(
            APPROVED_BRONZE_ROW_COUNT
        ),
        "processed_silver_row_count": int(
            processed_rows
        ),
        "started_at_utc": PROCESSED_AT_UTC.replace(
            tzinfo=None
        ),
        "completed_at_utc": (
            current_utc_timestamp()
            if run_status in ["COMPLETED", "FAILED"]
            else None
        ),
        "error_message": (
            str(error_message)[:4000]
            if error_message
            else None
        )
    }

    updated_df = dataframe_from_target_schema(
        RUN_LOG_TABLE,
        [updated_record]
    )

    merge_quality_records(
        target_table=RUN_LOG_TABLE,
        source_df=updated_df,
        merge_condition=(
            "target.silver_run_id = source.silver_run_id"
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 8 — Select approved metadata**
# 
# Definitions only. Validates destination uniqueness, source identity, status and complete batch coverage; rejects ambiguous retry records.

# CELL ********************

def prepare_manifest():
    global CONFIG_LOOKUP, active_config_count, APPROVED_BATCH_ID, APPROVED_BRONZE_ROW_COUNT
    configs = [r.asDict() for r in spark.table("wwi_bronze.ops.table_config").collect()
               if r["IsActive"] is True]
    active_config_count = len(configs)
    if active_config_count != EXPECTED_TABLE_COUNT: raise ValueError("Expected 48 active configurations")
    schema_map = {"Application":"reference", "Sales":"sales", "Purchasing":"purchasing", "Warehouse":"inventory"}
    keys, destinations = set(), set()
    for c in configs:
        key = (c["SourceSchema"], c["SourceTable"])
        dest = (c["SilverSchema"], c["SilverTable"])
        if key in keys or dest in destinations: raise ValueError("Duplicate source or destination configuration")
        keys.add(key); destinations.add(dest)
        if c["SourceSystem"] != "AZURE_SQL_WWI" or c["SourceDatabase"] != "free-sql-db-7745991":
            raise ValueError("Unexpected source identity")
        if c["SilverLakehouse"] != "wwi_silver" or schema_map.get(key[0]) != dest[0]:
            raise ValueError(f"Invalid destination: {dest}")
        if not re.fullmatch(r"[a-z][a-z0-9_]*", c["SilverTable"] or ""):
            raise ValueError("Invalid configured table identifier")
        if c["ConfigStatus"] not in {"READY_FOR_INCREMENTAL", "READY_HISTORY_PARENT_KEY_REQUIRED", "READY_FOR_FULL_RELOAD", "READY_FULL_RELOAD", "READY"}:
            raise ValueError(f"Review ConfigStatus: {c['ConfigStatus']}")
        if c["TableType"] not in {"HISTORY_TABLE", "SYSTEM_VERSIONED_TEMPORAL_TABLE", "NON_TEMPORAL_TABLE"}:
            raise ValueError("Unknown table type")
    CONFIG_LOOKUP = {(c["SourceSchema"], c["SourceTable"]): c for c in configs}
    logs = [r.asDict() for r in spark.table("wwi_bronze.ops.batch_log").collect()
            if (r["LoadType"] or "").upper() == "FULL"
            and r["SourceSystem"] == "AZURE_SQL_WWI"
            and r["SourceDatabase"] == "free-sql-db-7745991"]
    groups = {}
    for row in logs:
        if row["BatchID"] and (row["SourceSchema"], row["SourceTable"]) in keys:
            groups.setdefault(row["BatchID"], {}).setdefault((row["SourceSchema"], row["SourceTable"]), []).append(row)
    candidates = []
    for batch, tables in groups.items():
        if APPROVED_BATCH_ID and batch != APPROVED_BATCH_ID: continue
        if set(tables) != keys: continue
        selected = []; valid = True
        for key, rows in tables.items():
            # Most recently logged validation is authoritative; never silently ignore a later failure.
            stamp = lambda r: (r["LoggedAtUTC"] or datetime.min, r["ValidatedAtUTC"] or datetime.min)
            latest = max(map(stamp, rows))
            top = [r for r in rows if stamp(r) == latest]
            if len(top) != 1: valid = False; break
            r = top[0]
            if not ((r["BatchStatus"] or "").upper() == "VALIDATED"
                and r["SourceRowCount"] is not None and r["SourceRowCount"] >= 0
                and r["SourceRowCount"] == r["BronzeRowCount"] and r["RowCountDifference"] == 0
                and r["ValidatedAtUTC"] is not None and not (r["ErrorMessage"] or "").strip()
                and (r["ParquetFileCount"] or 0) > 0 and (r["BronzeColumnCount"] or 0) > 0
                and r["TableType"] == CONFIG_LOOKUP[key]["TableType"]):
                valid = False; break
            for name in ["BatchID", "AttemptID"]:
                if not re.fullmatch(r"[A-Za-z0-9_-]+", r[name] or ""): valid = False
            raw = (r["RawPathPattern"] or "").rstrip("/")
            if f"/{key[0]}/{key[1]}/load_type=full/" not in raw or not raw.endswith(f"/batch_id={batch}/attempt_id={r['AttemptID']}"):
                valid = False
            selected.append({**CONFIG_LOOKUP[key], **r})
        if valid: candidates.append((max(r["ValidatedAtUTC"] for r in selected), batch, selected))
    if not candidates: raise ValueError("No unambiguous complete validated FULL batch matches the request")
    candidates.sort(key=lambda x:x[0], reverse=True)
    if len(candidates)>1 and candidates[0][0] == candidates[1][0]: raise ValueError("Batch timestamp tie; supply p_batch_id")
    _, APPROVED_BATCH_ID, records = candidates[0]
    APPROVED_BRONZE_ROW_COUNT = sum(r["BronzeRowCount"] for r in records)
    return sorted(records, key=lambda r:(r["SourceSchema"],r["SourceTable"]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 9 — Resolve exact folders**
# 
# Definition only. Date wildcards resolve to one exact batch/attempt directory.

# CELL ********************

def resolve_approved_bronze_directory(manifest_record):
    source_schema = manifest_record["SourceSchema"]
    source_table = manifest_record["SourceTable"]
    batch_id = manifest_record["BatchID"]
    attempt_id = manifest_record["AttemptID"]

    path_pattern = (
        f"{BRONZE_ABFSS_ROOT}/Files/raw/wwi/"
        f"{source_schema}/{source_table}/"
        "load_type=full/"
        "ingest_year=*/ingest_month=*/ingest_day=*/"
        f"batch_id={batch_id}/"
        f"attempt_id={attempt_id}"
    )

    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(
        path_pattern
    )

    filesystem = hadoop_path.getFileSystem(
        spark._jsc.hadoopConfiguration()
    )

    matches = filesystem.globStatus(hadoop_path)

    matched_directories = [
        item.getPath().toString()
        for item in (matches or [])
        if item.isDirectory()
    ]

    if len(matched_directories) != 1:
        raise RuntimeError(
            f"{source_schema}.{source_table}: expected one "
            f"approved Bronze directory, found "
            f"{len(matched_directories)}. "
            f"Pattern={path_pattern}; "
            f"Matches={matched_directories}"
        )

    return matched_directories[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 10 — Automatic processing function**
# 
# Definition only. Sequential reads, validation, Delta overwrite and post-write checks. Stops on the first failure.

# CELL ********************

def process_tables(manifest_records, RESOLVED_BRONZE_PATHS):
    from pyspark import StorageLevel

    successful_tables = []
    processing_failed = False

    for table_number, manifest_record in enumerate(
        manifest_records,
        start=1
    ):
        source_schema = manifest_record["SourceSchema"]
        source_table = manifest_record["SourceTable"]

        silver_schema = manifest_record["SilverSchema"]
        silver_table = manifest_record["SilverTable"]

        target_table = (
            f"wwi_silver.{silver_schema}.{silver_table}"
        )

        current_stage = "INITIALIZATION"
        transformed_df = None
        validation = None

        print(
            f"\n[{table_number}/{EXPECTED_TABLE_COUNT}] "
            f"{source_schema}.{source_table} "
            f"→ {silver_schema}.{silver_table}"
        )

        try:
            # -----------------------------------------------------
            # 1. Read the exact approved Bronze directory
            # -----------------------------------------------------
            current_stage = "BRONZE_READ"

            bronze_path = RESOLVED_BRONZE_PATHS[
                (source_schema, source_table)
            ]

            bronze_df = (
                spark.read
                .option("recursiveFileLookup", "true")
                .option("pathGlobFilter", "*.parquet")
                .parquet(bronze_path)
            )

            actual_bronze_columns = len(bronze_df.columns)

            if actual_bronze_columns != int(
                manifest_record["BronzeColumnCount"]
            ):
                raise ValueError(
                    f"Bronze column mismatch: expected "
                    f"{manifest_record['BronzeColumnCount']}, "
                    f"found {actual_bronze_columns}."
                )

            # -----------------------------------------------------
            # 2. Transform to the standard Silver structure
            # -----------------------------------------------------
            current_stage = "TRANSFORMATION"

            transformed_df, datetime_columns = (
                transform_bronze_to_silver(
                    bronze_df=bronze_df,
                    source_schema=source_schema,
                    source_table=source_table,
                    batch_id=APPROVED_BATCH_ID,
                    attempt_id=manifest_record["AttemptID"],
                    silver_run_id=SILVER_RUN_ID,
                    processed_at_utc=PROCESSED_AT_UTC
                )
            )

            transformed_df.persist(
                StorageLevel.MEMORY_AND_DISK
            )

            # -----------------------------------------------------
            # 3. Validate business keys and transformed data
            # -----------------------------------------------------
            current_stage = "QUALITY_VALIDATION"

            validation_keys = get_silver_validation_keys(
                manifest_record
            )

            validation = validate_silver_dataframe(
                silver_df=transformed_df,
                expected_row_count=manifest_record[
                    "BronzeRowCount"
                ],
                expected_bronze_column_count=manifest_record[
                    "BronzeColumnCount"
                ],
                primary_key_columns=validation_keys,
                datetime_columns=datetime_columns
            )

            # Check that all binary columns stayed binary
            bronze_binary_columns = [
                to_snake_case(column_name)
                for column_name, data_type in bronze_df.dtypes
                if data_type == "binary"
            ]

            transformed_types = dict(
                transformed_df.dtypes
            )

            changed_binary_columns = [
                column_name
                for column_name in bronze_binary_columns
                if transformed_types.get(column_name) != "binary"
            ]

            validation["binary_type_changes"] = len(
                changed_binary_columns
            )

            validation["passed"] = (
                validation["passed"]
                and validation["binary_type_changes"] == 0
            )

            save_validation_results(
                manifest_record,
                validation
            )

            if not validation["passed"]:
                raise ValueError(
                    f"Quality validation failed. "
                    f"Details={validation}"
                )

            # -----------------------------------------------------
            # 4. Write the managed Silver Delta table
            # -----------------------------------------------------
            current_stage = "DELTA_WRITE"

            (
                transformed_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(target_table)
            )

            # -----------------------------------------------------
            # 5. Re-read and verify the physical table
            # -----------------------------------------------------
            current_stage = "POST_WRITE_VALIDATION"

            written_df = spark.table(target_table)
            written_rows = written_df.count()
            written_columns = len(written_df.columns)

            if written_rows != validation["actual_row_count"]:
                raise ValueError(
                    f"Post-write row mismatch: expected "
                    f"{validation['actual_row_count']}, "
                    f"found {written_rows}."
                )

            if written_columns != validation[
                "actual_silver_column_count"
            ]:
                raise ValueError(
                    f"Post-write column mismatch: expected "
                    f"{validation['actual_silver_column_count']}, "
                    f"found {written_columns}."
                )

            incorrect_batch_rows = (
                written_df
                .filter(
                    (~F.col("_batch_id").eqNullSafe(F.lit(APPROVED_BATCH_ID)))
                    | (~F.col("_silver_run_id").eqNullSafe(F.lit(SILVER_RUN_ID)))
                )
                .limit(1)
                .count()
            )

            if incorrect_batch_rows > 0:
                raise ValueError(
                    "The written table contains an unexpected BatchID."
                )

            # -----------------------------------------------------
            # 6. Record success
            # -----------------------------------------------------
            current_stage = "QUALITY_LOGGING"

            save_table_result(
                manifest_record=manifest_record,
                validation=validation,
                table_status="VALIDATED"
            )

            successful_tables.append(
                f"{source_schema}.{source_table}"
            )

            update_silver_run("RUNNING")

            print(
                f"PASSED: {written_rows:,} rows written to "
                f"{silver_schema}.{silver_table}"
            )

        except Exception as table_error:
            processing_failed = True

            try:
                save_failure(
                    manifest_record=manifest_record,
                    processing_stage=current_stage,
                    error=table_error
                )

                save_table_result(
                    manifest_record=manifest_record,
                    validation=validation,
                    table_status="FAILED",
                    error_message=table_error
                )

                update_silver_run(
                    run_status="FAILED",
                    error_message=(
                        f"{source_schema}.{source_table}: "
                        f"{str(table_error)}"
                    )
                )

            except Exception as logging_error:
                print(
                    f"Quality logging also failed: "
                    f"{logging_error}"
                )

            raise RuntimeError(
                f"Silver processing stopped at "
                f"{source_schema}.{source_table}, "
                f"stage {current_stage}."
            ) from table_error

        finally:
            if transformed_df is not None:
                transformed_df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 11 — Execute and reconcile**
# 
# This is the only execution cell that writes business tables. Expected: 48 validated tables, zero failures, row total matching the selected batch. Any exception marks the run FAILED where logging is available.

# CELL ********************

RUN_COMPLETED = False
current_manifest = {"SourceSchema": "__RUN__", "SourceTable": "__RUN__"}
try:
    update_silver_run("RUNNING")
    manifest_records = prepare_manifest()
    update_silver_run("RUNNING")
    RESOLVED_BRONZE_PATHS = {}
    for current_manifest in manifest_records:
        key = (current_manifest["SourceSchema"], current_manifest["SourceTable"])
        get_silver_validation_keys(current_manifest)
        RESOLVED_BRONZE_PATHS[key] = resolve_approved_bronze_directory(current_manifest)
    print(f"Approved FULL batch: {APPROVED_BATCH_ID}; tables: {len(manifest_records)}; rows: {APPROVED_BRONZE_ROW_COUNT:,}")
    process_tables(manifest_records, RESOLVED_BRONZE_PATHS)
    current_manifest = {"SourceSchema": "__RUN__", "SourceTable": "__RUN__"}
    current_run_results_df = (
        spark.table(TABLE_RESULTS_TABLE)
        .filter(
            F.col("silver_run_id") == SILVER_RUN_ID
        )
    )

    validated_table_count = (
        current_run_results_df
        .filter(F.col("table_status") == "VALIDATED")
        .count()
    )

    failed_table_count = (
        current_run_results_df
        .filter(F.col("table_status") == "FAILED")
        .count()
    )

    total_silver_rows = (
        current_run_results_df
        .filter(F.col("table_status") == "VALIDATED")
        .agg(
            F.sum("actual_row_count").alias("total_rows")
        )
        .first()["total_rows"]
    )

    if validated_table_count != EXPECTED_TABLE_COUNT:
        raise RuntimeError(
            f"Expected {EXPECTED_TABLE_COUNT} validated tables, "
            f"found {validated_table_count}."
        )

    if failed_table_count != 0:
        raise RuntimeError(
            f"Found {failed_table_count} failed tables."
        )

    if total_silver_rows != APPROVED_BRONZE_ROW_COUNT:
        raise RuntimeError(
            f"Expected {APPROVED_BRONZE_ROW_COUNT:,} total rows, "
            f"found {total_silver_rows:,}."
        )



    print(f"Validated tables : {validated_table_count}")
    print(f"Failed tables    : {failed_table_count}")
    print(f"Silver rows      : {total_silver_rows:,}")
    print("Initial Bronze-to-Silver full load completed.")
    actual_sources = {(r["source_schema"], r["source_table"]) for r in current_run_results_df.collect()}
    if actual_sources != set(CONFIG_LOOKUP): raise ValueError("Final table-set reconciliation failed")
    update_silver_run("COMPLETED")
    RUN_COMPLETED = True
except Exception as run_error:
    for action in [lambda: save_failure(current_manifest, "RUN_ORCHESTRATION", run_error),
                   lambda: update_silver_run("FAILED", str(run_error))]:
        try: action()
        except Exception as log_error: print("Logging failure:", str(log_error)[:500])
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 12 — Return result**
# 
# Expected: COMPLETED JSON. Pipeline exit is outside exception handling so Fabric can return the value normally.

# CELL ********************

if not RUN_COMPLETED: raise RuntimeError("Run has not completed successfully")
result = json.dumps({"status":"COMPLETED", "silver_run_id":SILVER_RUN_ID,
                     "batch_id":APPROVED_BATCH_ID, "tables":validated_table_count,
                     "rows":int(total_silver_rows)})
print(result)
if str(p_pipeline_exit).lower() == "true":
    notebookutils.notebook.exit(result)

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
