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

p_batch_id = "20f2f6b1-aea1-4093-a63f-f649bc5447c8"
p_ingestion_attempt_id = "01"
p_validation_attempt_id = "01"

p_source_system = "AZURE_SQL_WWI"
p_source_database = "free-sql-db-7745991"

p_manifest_root = "Files/manifests/wwi/full_load_validation"

if not p_batch_id or p_batch_id == "PASTE_MASTER_PIPELINE_RUN_ID":
    raise ValueError("Enter the validated master pipeline Run ID.")

print(f"Batch ID           : {p_batch_id}")
print(f"Source system       : {p_source_system}")
print(f"Source database     : {p_source_database}")
print(f"Ingestion attempt   : {p_ingestion_attempt_id}")
print(f"Validation attempt  : {p_validation_attempt_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Imports and validated manifest</mark>

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F
import re
import json

table_manifest_path = (
    f"{p_manifest_root}/"
    f"batch_id={p_batch_id}/"
    f"validation_attempt_id={p_validation_attempt_id}/"
    "table_results"
)

print(f"Reading manifest from:\n{table_manifest_path}")

manifest_df = spark.read.parquet(table_manifest_path)

manifest_table_count = manifest_df.count()

display(
    manifest_df.orderBy("SchemaName", "TableName")
)

assert manifest_table_count == 48, (
    f"Expected 48 manifest records, found {manifest_table_count}."
)

failed_manifest_rows = (
    manifest_df
    .filter(~F.col("Status").startswith("PASS"))
    .count()
)

assert failed_manifest_rows == 0, (
    f"{failed_manifest_rows} tables failed validation."
)

print("PASS: Validated manifest contains all 48 tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Create the four operational Delta tables</mark>

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.table_config
(
    ConfigID STRING,
    SourceSystem STRING,
    SourceDatabase STRING,
    SourceSchema STRING,
    SourceTable STRING,
    TableType STRING,

    PrimaryKeyColumns STRING,
    InitialLoadMethod STRING,
    IncrementalEnabled BOOLEAN,
    IncrementalMethod STRING,
    WatermarkColumn STRING,

    BronzeBasePath STRING,
    SilverLakehouse STRING,
    SilverSchema STRING,
    SilverTable STRING,

    IsActive BOOLEAN,
    ConfigStatus STRING,
    CreatedAtUTC TIMESTAMP,
    UpdatedAtUTC TIMESTAMP
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.batch_log
(
    BatchID STRING,
    AttemptID STRING,
    ValidationAttemptID STRING,
    LoadType STRING,

    SourceSystem STRING,
    SourceDatabase STRING,
    SourceSchema STRING,
    SourceTable STRING,
    TableType STRING,

    SourceRowCount BIGINT,
    BronzeRowCount BIGINT,
    RowCountDifference BIGINT,
    ParquetFileCount INT,
    BronzeColumnCount INT,

    RawPathPattern STRING,
    BatchStatus STRING,
    ErrorMessage STRING,

    ValidatedAtUTC TIMESTAMP,
    LoggedAtUTC TIMESTAMP
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.watermarks
(
    SourceSystem STRING,
    SourceDatabase STRING,
    SourceSchema STRING,
    SourceTable STRING,

    WatermarkColumn STRING,
    PreviousWatermark STRING,
    CurrentWatermark STRING,

    LastSuccessfulBatchID STRING,
    WatermarkStatus STRING,
    UpdatedAtUTC TIMESTAMP
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS ops.generation_runs
(
    GenerationRunID STRING,
    SourceDatabase STRING,

    RequestedStartDate DATE,
    RequestedEndDate DATE,

    SourceVersion STRING,
    ResetMode STRING,
    ParametersJSON STRING,

    RunStatus STRING,
    RequestedAtUTC TIMESTAMP,
    StartedAtUTC TIMESTAMP,
    CompletedAtUTC TIMESTAMP,

    RowsGenerated BIGINT,
    ErrorMessage STRING
)
USING DELTA
""")

print("Operational tables created successfully:")
print(" - ops.table_config")
print(" - ops.batch_log")
print(" - ops.watermarks")
print(" - ops.generation_runs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Define the 48 source tables and table types</mark>

# CELL ********************

source_tables = [
    ("Application", "Cities", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Application", "Cities_Archive", "HISTORY_TABLE"),
    ("Application", "Countries", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Application", "Countries_Archive", "HISTORY_TABLE"),
    ("Application", "DeliveryMethods", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Application", "DeliveryMethods_Archive", "HISTORY_TABLE"),
    ("Application", "PaymentMethods", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Application", "PaymentMethods_Archive", "HISTORY_TABLE"),
    ("Application", "People", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Application", "People_Archive", "HISTORY_TABLE"),
    ("Application", "StateProvinces", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Application", "StateProvinces_Archive", "HISTORY_TABLE"),
    ("Application", "SystemParameters", "NON_TEMPORAL_TABLE"),
    ("Application", "TransactionTypes", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Application", "TransactionTypes_Archive", "HISTORY_TABLE"),

    ("Purchasing", "PurchaseOrderLines", "NON_TEMPORAL_TABLE"),
    ("Purchasing", "PurchaseOrders", "NON_TEMPORAL_TABLE"),
    ("Purchasing", "SupplierCategories", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Purchasing", "SupplierCategories_Archive", "HISTORY_TABLE"),
    ("Purchasing", "Suppliers", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Purchasing", "Suppliers_Archive", "HISTORY_TABLE"),
    ("Purchasing", "SupplierTransactions", "NON_TEMPORAL_TABLE"),

    ("Sales", "BuyingGroups", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Sales", "BuyingGroups_Archive", "HISTORY_TABLE"),
    ("Sales", "CustomerCategories", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Sales", "CustomerCategories_Archive", "HISTORY_TABLE"),
    ("Sales", "Customers", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Sales", "Customers_Archive", "HISTORY_TABLE"),
    ("Sales", "CustomerTransactions", "NON_TEMPORAL_TABLE"),
    ("Sales", "InvoiceLines", "NON_TEMPORAL_TABLE"),
    ("Sales", "Invoices", "NON_TEMPORAL_TABLE"),
    ("Sales", "OrderLines", "NON_TEMPORAL_TABLE"),
    ("Sales", "Orders", "NON_TEMPORAL_TABLE"),
    ("Sales", "SpecialDeals", "NON_TEMPORAL_TABLE"),

    ("Warehouse", "ColdRoomTemperatures", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Warehouse", "ColdRoomTemperatures_Archive", "HISTORY_TABLE"),
    ("Warehouse", "Colors", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Warehouse", "Colors_Archive", "HISTORY_TABLE"),
    ("Warehouse", "PackageTypes", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Warehouse", "PackageTypes_Archive", "HISTORY_TABLE"),
    ("Warehouse", "StockGroups", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Warehouse", "StockGroups_Archive", "HISTORY_TABLE"),
    ("Warehouse", "StockItemHoldings", "NON_TEMPORAL_TABLE"),
    ("Warehouse", "StockItems", "SYSTEM_VERSIONED_TEMPORAL_TABLE"),
    ("Warehouse", "StockItems_Archive", "HISTORY_TABLE"),
    ("Warehouse", "StockItemStockGroups", "NON_TEMPORAL_TABLE"),
    ("Warehouse", "StockItemTransactions", "NON_TEMPORAL_TABLE"),
    ("Warehouse", "VehicleTemperatures", "NON_TEMPORAL_TABLE")
]

assert len(source_tables) == 48, (
    f"Expected 48 table configurations, found {len(source_tables)}."
)

source_types_df = spark.createDataFrame(
    source_tables,
    ["SourceSchema", "SourceTable", "TableType"]
)

display(
    source_types_df.orderBy("SourceSchema", "SourceTable")
)

print("PASS: Defined metadata for 48 source tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Prepare the table_config records</mark>

# CELL ********************

# Cell 5: Prepare table_config records

def to_snake_case(name):
    value = re.sub(
        r"(?<!^)(?=[A-Z])",
        "_",
        name
    )

    return value.lower()


silver_schema_mapping = {
    "Application": "reference",
    "Purchasing": "purchasing",
    "Sales": "sales",
    "Warehouse": "inventory"
}

config_seed_rows = []

for source_schema, source_table, table_type in source_tables:
    config_id = (
        f"{p_source_system}|"
        f"{source_schema}|"
        f"{source_table}"
    )

    bronze_base_path = (
        f"Files/raw/wwi/"
        f"{source_schema}/"
        f"{source_table}"
    )

    silver_schema = silver_schema_mapping[source_schema]
    silver_table = to_snake_case(source_table)

    # Do not add the all-null columns here because
    # PySpark would be unable to infer their types.
    config_seed_rows.append({
        "ConfigID": config_id,
        "SourceSystem": p_source_system,
        "SourceDatabase": p_source_database,
        "SourceSchema": source_schema,
        "SourceTable": source_table,
        "TableType": table_type,

        "InitialLoadMethod": "FULL",
        "IncrementalEnabled": False,
        "IncrementalMethod": "PENDING",

        "BronzeBasePath": bronze_base_path,
        "SilverLakehouse": "wwi_silver",
        "SilverSchema": silver_schema,
        "SilverTable": silver_table,

        "IsActive": True,
        "ConfigStatus": "FULL_VALIDATED_INCREMENTAL_PENDING"
    })


config_seed_df = (
    spark.createDataFrame(config_seed_rows)

    # Add all-null columns with explicit data types.
    .withColumn(
        "PrimaryKeyColumns",
        F.lit(None).cast("string")
    )
    .withColumn(
        "WatermarkColumn",
        F.lit(None).cast("string")
    )
    .withColumn(
        "CreatedAtUTC",
        F.current_timestamp()
    )
    .withColumn(
        "UpdatedAtUTC",
        F.current_timestamp()
    )

    # Arrange columns in the same order as ops.table_config.
    .select(
        "ConfigID",
        "SourceSystem",
        "SourceDatabase",
        "SourceSchema",
        "SourceTable",
        "TableType",
        "PrimaryKeyColumns",
        "InitialLoadMethod",
        "IncrementalEnabled",
        "IncrementalMethod",
        "WatermarkColumn",
        "BronzeBasePath",
        "SilverLakehouse",
        "SilverSchema",
        "SilverTable",
        "IsActive",
        "ConfigStatus",
        "CreatedAtUTC",
        "UpdatedAtUTC"
    )
)

display(
    config_seed_df.orderBy(
        "SourceSchema",
        "SourceTable"
    )
)

assert config_seed_df.count() == 48, (
    f"Expected 48 configuration rows, "
    f"found {config_seed_df.count()}."
)

print("PASS: Prepared 48 table-configuration records.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Insert the table configuration idempotently</mark>

# CELL ********************

table_config_target = DeltaTable.forName(
    spark,
    "ops.table_config"
)

(
    table_config_target.alias("target")
    .merge(
        config_seed_df.alias("source"),
        "target.ConfigID = source.ConfigID"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

configured_table_count = (
    spark.table("ops.table_config")
    .filter(
        F.col("SourceSystem") == p_source_system
    )
    .count()
)

print(f"Configured WWI tables: {configured_table_count}")

assert configured_table_count == 48, (
    f"Expected 48 configurations, found {configured_table_count}."
)

print("PASS: ops.table_config contains all 48 WWI tables.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Prepare the initial full-load batch log</mark>

# CELL ********************

# Cell 7: Prepare the initial full-load batch log

# Standardize the manifest column names before joining.
manifest_normalized_df = (
    manifest_df
    .withColumnRenamed(
        "SchemaName",
        "SourceSchema"
    )
    .withColumnRenamed(
        "TableName",
        "SourceTable"
    )
)

batch_log_df = (
    manifest_normalized_df

    # Add the source table type.
    .join(
        source_types_df,
        on=["SourceSchema", "SourceTable"],
        how="left"
    )

    # Operational identifiers.
    .withColumn(
        "BatchID",
        F.lit(p_batch_id)
    )
    .withColumn(
        "AttemptID",
        F.lit(p_ingestion_attempt_id)
    )
    .withColumn(
        "ValidationAttemptID",
        F.lit(p_validation_attempt_id)
    )
    .withColumn(
        "LoadType",
        F.lit("FULL")
    )

    # Source details.
    .withColumn(
        "SourceSystem",
        F.lit(p_source_system)
    )
    .withColumn(
        "SourceDatabase",
        F.lit(p_source_database)
    )

    # Row-count validation results.
    .withColumn(
        "SourceRowCount",
        F.col("ExpectedRows").cast("long")
    )
    .withColumn(
        "BronzeRowCount",
        F.col("ActualRows").cast("long")
    )
    .withColumn(
        "RowCountDifference",
        F.col("Difference").cast("long")
    )
    .withColumn(
        "ParquetFileCount",
        F.col("ParquetFiles").cast("int")
    )
    .withColumn(
        "BronzeColumnCount",
        F.col("ColumnCount").cast("int")
    )

    # Logical location of the raw batch.
    .withColumn(
        "RawPathPattern",
        F.concat(
            F.lit("Files/raw/wwi/"),
            F.col("SourceSchema"),
            F.lit("/"),
            F.col("SourceTable"),
            F.lit("/load_type=full/.../batch_id="),
            F.lit(p_batch_id),
            F.lit("/attempt_id="),
            F.lit(p_ingestion_attempt_id)
        )
    )

    # Convert validation results into operational status.
    .withColumn(
        "BatchStatus",
        F.when(
            F.col("Status").startswith("PASS"),
            F.lit("VALIDATED")
        ).otherwise(
            F.lit("VALIDATION_FAILED")
        )
    )
    .withColumn(
        "LoggedAtUTC",
        F.current_timestamp()
    )

    # Match the ops.batch_log table structure.
    .select(
        "BatchID",
        "AttemptID",
        "ValidationAttemptID",
        "LoadType",
        "SourceSystem",
        "SourceDatabase",
        "SourceSchema",
        "SourceTable",
        "TableType",
        "SourceRowCount",
        "BronzeRowCount",
        "RowCountDifference",
        "ParquetFileCount",
        "BronzeColumnCount",
        "RawPathPattern",
        "BatchStatus",
        "ErrorMessage",
        "ValidatedAtUTC",
        "LoggedAtUTC"
    )
)

display(
    batch_log_df.orderBy(
        "SourceSchema",
        "SourceTable"
    )
)

assert batch_log_df.count() == 48, (
    f"Expected 48 batch-log rows, "
    f"found {batch_log_df.count()}."
)

missing_table_types = (
    batch_log_df
    .filter(F.col("TableType").isNull())
    .count()
)

assert missing_table_types == 0, (
    f"{missing_table_types} records are missing TableType."
)

print("PASS: Prepared 48 initial batch-log records.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Merge the batch log into ops.batch_log</mark>

# CELL ********************

batch_log_target = DeltaTable.forName(
    spark,
    "ops.batch_log"
)

batch_log_merge_condition = """
    target.BatchID = source.BatchID
    AND target.AttemptID = source.AttemptID
    AND target.SourceSchema = source.SourceSchema
    AND target.SourceTable = source.SourceTable
"""

(
    batch_log_target.alias("target")
    .merge(
        batch_log_df.alias("source"),
        batch_log_merge_condition
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

logged_batch_df = (
    spark.table("ops.batch_log")
    .filter(
        F.col("BatchID") == p_batch_id
    )
)

logged_table_count = logged_batch_df.count()

print(f"Batch-log records created: {logged_table_count}")

assert logged_table_count == 48, (
    f"Expected 48 batch-log records, found {logged_table_count}."
)

print("PASS: Initial full-load batch was registered.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Verify all operational tables</mark>

# CELL ********************

operational_tables = [
    "ops.table_config",
    "ops.batch_log",
    "ops.watermarks",
    "ops.generation_runs"
]

verification_rows = []

for table_name in operational_tables:
    exists = spark.catalog.tableExists(table_name)

    row_count = (
        spark.table(table_name).count()
        if exists
        else None
    )

    verification_rows.append({
        "TableName": table_name,
        "Exists": exists,
        "RowCount": row_count
    })

verification_df = spark.createDataFrame(
    verification_rows
)

display(verification_df)

assert all(
    row["Exists"]
    for row in verification_rows
), "One or more operational tables were not created."

assert (
    spark.table("ops.table_config").count() == 48
), "table_config must contain 48 rows."

assert (
    logged_batch_df
    .filter(F.col("BatchStatus") != "VALIDATED")
    .count() == 0
), "One or more batch records are not validated."

print("PASS: Operational control layer is initialized.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

setup_result = {
    "batch_id": p_batch_id,
    "configured_tables": spark.table(
        "ops.table_config"
    ).count(),
    "batch_log_records": logged_table_count,
    "watermark_records": spark.table(
        "ops.watermarks"
    ).count(),
    "generation_records": spark.table(
        "ops.generation_runs"
    ).count(),
    "status": "PASS"
}

print(
    json.dumps(
        setup_result,
        indent=2
    )
)

print("\nBRONZE OPERATIONAL CONTROL LAYER IS READY.")

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
