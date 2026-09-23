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

# PARAMETERS

p_metadata_json = "[]"
p_source_system = "AZURE_SQL_WWI"

print(f"Source system: {p_source_system}")
print(f"Metadata JSON length: {len(p_metadata_json)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Parse the Lookup JSON

# CELL ********************

import json

from delta.tables import DeltaTable

from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType
)


def normalize_boolean(value):
    if isinstance(value, bool):
        return value

    if value in [1, "1", "true", "True", "TRUE"]:
        return True

    return False


metadata_records = json.loads(p_metadata_json)

if not isinstance(metadata_records, list):
    raise ValueError(
        "p_metadata_json must contain a JSON array."
    )

if len(metadata_records) == 0:
    raise ValueError(
        "No metadata records were received from the Lookup activity."
    )


metadata_rows = []

for record in metadata_records:
    metadata_rows.append({
        "SourceSchema": record.get("SourceSchema"),
        "SourceTable": record.get("SourceTable"),
        "TableType": record.get("TableType"),
        "PrimaryKeyColumns": record.get("PrimaryKeyColumns"),
        "IncrementalEnabled": normalize_boolean(
            record.get("IncrementalEnabled")
        ),
        "IncrementalMethod": record.get("IncrementalMethod"),
        "WatermarkColumn": record.get("WatermarkColumn"),
        "ConfigStatus": record.get("ConfigStatus")
    })


metadata_schema = StructType([
    StructField("SourceSchema", StringType(), False),
    StructField("SourceTable", StringType(), False),
    StructField("TableType", StringType(), False),
    StructField("PrimaryKeyColumns", StringType(), True),
    StructField("IncrementalEnabled", BooleanType(), False),
    StructField("IncrementalMethod", StringType(), False),
    StructField("WatermarkColumn", StringType(), True),
    StructField("ConfigStatus", StringType(), False)
])


metadata_df = spark.createDataFrame(
    metadata_rows,
    schema=metadata_schema
)

metadata_count = metadata_df.count()

display(
    metadata_df.orderBy(
        "SourceSchema",
        "SourceTable"
    )
)

assert metadata_count == 48, (
    f"Expected 48 metadata records, "
    f"received {metadata_count}."
)

print("PASS: Received metadata for 48 tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Prepare metadata updates</mark>

# CELL ********************

metadata_update_df = (
    metadata_df
    .withColumn(
        "ConfigID",
        F.concat_ws(
            "|",
            F.lit(p_source_system),
            F.col("SourceSchema"),
            F.col("SourceTable")
        )
    )
)

existing_config_df = spark.table(
    "ops.table_config"
)

unmatched_metadata_df = (
    metadata_update_df.alias("metadata")
    .join(
        existing_config_df
        .select("ConfigID")
        .alias("config"),
        on="ConfigID",
        how="left_anti"
    )
)

unmatched_count = unmatched_metadata_df.count()

if unmatched_count > 0:
    print("Metadata without matching table configuration:")

    display(unmatched_metadata_df)

assert unmatched_count == 0, (
    f"{unmatched_count} metadata records do not have "
    "matching table_config records."
)

print("PASS: All metadata records match table_config.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Merge metadata into ops.table_config</mark>

# CELL ********************

table_config_target = DeltaTable.forName(
    spark,
    "ops.table_config"
)

merge_condition = (
    "target.ConfigID = source.ConfigID"
)

(
    table_config_target.alias("target")
    .merge(
        metadata_update_df.alias("source"),
        merge_condition
    )
    .whenMatchedUpdate(
        set={
            "TableType": "source.TableType",
            "PrimaryKeyColumns": (
                "source.PrimaryKeyColumns"
            ),
            "IncrementalEnabled": (
                "source.IncrementalEnabled"
            ),
            "IncrementalMethod": (
                "source.IncrementalMethod"
            ),
            "WatermarkColumn": (
                "source.WatermarkColumn"
            ),
            "ConfigStatus": (
                "source.ConfigStatus"
            ),
            "UpdatedAtUTC": "current_timestamp()"
        }
    )
    .execute()
)

print("Metadata merge completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Validate the updated configuration</mark>

# CELL ********************

updated_config_df = (
    spark.table("ops.table_config")
    .filter(
        F.col("SourceSystem") == p_source_system
    )
)

configured_count = updated_config_df.count()

pending_method_count = (
    updated_config_df
    .filter(
        F.col("IncrementalMethod").isNull()
        | (F.col("IncrementalMethod") == "PENDING")
    )
    .count()
)

non_history_missing_pk_count = (
    updated_config_df
    .filter(
        (F.col("TableType") != "HISTORY_TABLE")
        & F.col("PrimaryKeyColumns").isNull()
    )
    .count()
)

incremental_missing_watermark_count = (
    updated_config_df
    .filter(
        (F.col("IncrementalEnabled") == True)
        & F.col("WatermarkColumn").isNull()
    )
    .count()
)

print(f"Configured tables              : {configured_count}")
print(f"Pending incremental methods    : {pending_method_count}")
print(
    "Non-history tables missing PK : "
    f"{non_history_missing_pk_count}"
)
print(
    "Incremental tables missing WM : "
    f"{incremental_missing_watermark_count}"
)

assert configured_count == 48, (
    f"Expected 48 configurations, found {configured_count}."
)

assert pending_method_count == 0, (
    f"{pending_method_count} tables still have a pending method."
)

assert non_history_missing_pk_count == 0, (
    f"{non_history_missing_pk_count} non-history tables "
    "are missing primary keys."
)

assert incremental_missing_watermark_count == 0, (
    f"{incremental_missing_watermark_count} incremental "
    "tables are missing watermark columns."
)

print("PASS: Table configuration metadata is valid.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Review incremental strategies</mark>

# CELL ********************

display(
    updated_config_df
    .groupBy(
        "TableType",
        "IncrementalMethod",
        "ConfigStatus"
    )
    .count()
    .orderBy(
        "TableType",
        "IncrementalMethod"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    updated_config_df
    .select(
        "SourceSchema",
        "SourceTable",
        "TableType",
        "PrimaryKeyColumns",
        "IncrementalEnabled",
        "IncrementalMethod",
        "WatermarkColumn",
        "ConfigStatus"
    )
    .orderBy(
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

metadata_result = {
    "received_records": metadata_count,
    "configured_tables": configured_count,
    "pending_methods": pending_method_count,
    "non_history_missing_primary_keys": (
        non_history_missing_pk_count
    ),
    "incremental_missing_watermarks": (
        incremental_missing_watermark_count
    ),
    "status": "PASS"
}

print(
    json.dumps(
        metadata_result,
        indent=2
    )
)

print("\nTABLE CONFIGURATION METADATA IS READY.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
