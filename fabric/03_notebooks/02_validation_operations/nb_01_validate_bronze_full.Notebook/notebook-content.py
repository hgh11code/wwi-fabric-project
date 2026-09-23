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
p_load_type = "full"
p_attempt_id = "01"
p_validation_attempt_id = "01"
p_raw_root = "Files/raw/wwi"

if not p_batch_id or p_batch_id == "PASTE_MASTER_PIPELINE_RUN_ID":
    raise ValueError("Enter the master pipeline Run ID in p_batch_id.")

print("Validation parameters")
print("---------------------")
print(f"Batch ID             : {p_batch_id}")
print(f"Load type            : {p_load_type}")
print(f"Ingestion attempt    : {p_attempt_id}")
print(f"Validation attempt   : {p_validation_attempt_id}")
print(f"Raw root             : {p_raw_root}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Imports and expected source baseline</mark>

# CELL ********************

from collections import defaultdict
from pyspark.sql import functions as F
import json

expected_counts = {
    ("Application", "Cities"): 37940,
    ("Application", "Cities_Archive"): 28,
    ("Application", "Countries"): 190,
    ("Application", "Countries_Archive"): 37,
    ("Application", "DeliveryMethods"): 10,
    ("Application", "DeliveryMethods_Archive"): 1,
    ("Application", "PaymentMethods"): 4,
    ("Application", "PaymentMethods_Archive"): 1,
    ("Application", "People"): 1111,
    ("Application", "People_Archive"): 961,
    ("Application", "StateProvinces"): 53,
    ("Application", "StateProvinces_Archive"): 104,
    ("Application", "SystemParameters"): 1,
    ("Application", "TransactionTypes"): 13,
    ("Application", "TransactionTypes_Archive"): 1,

    ("Purchasing", "PurchaseOrderLines"): 8367,
    ("Purchasing", "PurchaseOrders"): 2074,
    ("Purchasing", "SupplierCategories"): 9,
    ("Purchasing", "SupplierCategories_Archive"): 1,
    ("Purchasing", "Suppliers"): 13,
    ("Purchasing", "Suppliers_Archive"): 13,
    ("Purchasing", "SupplierTransactions"): 2438,

    ("Sales", "BuyingGroups"): 2,
    ("Sales", "BuyingGroups_Archive"): 0,
    ("Sales", "CustomerCategories"): 8,
    ("Sales", "CustomerCategories_Archive"): 1,
    ("Sales", "Customers"): 663,
    ("Sales", "Customers_Archive"): 51,
    ("Sales", "CustomerTransactions"): 97147,
    ("Sales", "InvoiceLines"): 228265,
    ("Sales", "Invoices"): 70510,
    ("Sales", "OrderLines"): 231412,
    ("Sales", "Orders"): 73595,
    ("Sales", "SpecialDeals"): 2,

    ("Warehouse", "ColdRoomTemperatures"): 4,
    ("Warehouse", "ColdRoomTemperatures_Archive"): 3654736,
    ("Warehouse", "Colors"): 36,
    ("Warehouse", "Colors_Archive"): 1,
    ("Warehouse", "PackageTypes"): 14,
    ("Warehouse", "PackageTypes_Archive"): 0,
    ("Warehouse", "StockGroups"): 10,
    ("Warehouse", "StockGroups_Archive"): 1,
    ("Warehouse", "StockItemHoldings"): 227,
    ("Warehouse", "StockItems"): 227,
    ("Warehouse", "StockItems_Archive"): 444,
    ("Warehouse", "StockItemStockGroups"): 442,
    ("Warehouse", "StockItemTransactions"): 236667,
    ("Warehouse", "VehicleTemperatures"): 65998
}

expected_schemas = [
    "Application",
    "Purchasing",
    "Sales",
    "Warehouse"
]

expected_table_count = len(expected_counts)
expected_total_rows = sum(expected_counts.values())

print(f"Expected schemas : {len(expected_schemas)}")
print(f"Expected tables  : {expected_table_count}")
print(f"Expected rows    : {expected_total_rows:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Validate the source-schema folders</mark>

# CELL ********************

raw_items = notebookutils.fs.ls(p_raw_root)

actual_schemas = sorted([
    item.name.rstrip("/")
    for item in raw_items
    if item.isDir
])

expected_schemas_sorted = sorted(expected_schemas)

print("Expected schemas:", expected_schemas_sorted)
print("Actual schemas  :", actual_schemas)

missing_schemas = sorted(
    set(expected_schemas_sorted) - set(actual_schemas)
)

unexpected_schemas = sorted(
    set(actual_schemas) - set(expected_schemas_sorted)
)

print("Missing schemas   :", missing_schemas)
print("Unexpected schemas:", unexpected_schemas)

assert not missing_schemas, \
    f"Missing Bronze schemas: {missing_schemas}"

assert not unexpected_schemas, \
    f"Unexpected Bronze schemas: {unexpected_schemas}"

print("PASS: Bronze schema-folder structure is correct.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Recursively discover the batch’s Parquet files</mark>

# CELL ********************

def list_parquet_files_recursively(root_path):
    parquet_files = []
    folders_to_scan = [root_path]

    while folders_to_scan:
        current_folder = folders_to_scan.pop()

        for item in notebookutils.fs.ls(current_folder):
            if item.isDir:
                folders_to_scan.append(item.path)

            elif item.isFile and item.name.lower().endswith(".parquet"):
                parquet_files.append(item.path)

    return parquet_files


all_parquet_files = list_parquet_files_recursively(p_raw_root)

load_type_marker = f"/load_type={p_load_type}/"
batch_marker = f"/batch_id={p_batch_id}/"
attempt_marker = f"/attempt_id={p_attempt_id}/"

batch_files = [
    path
    for path in all_parquet_files
    if load_type_marker in path
    and batch_marker in path
    and attempt_marker in path
]

print(f"All Parquet files found : {len(all_parquet_files)}")
print(f"Files for current batch : {len(batch_files)}")

assert len(batch_files) > 0, (
    f"No Parquet files found for batch {p_batch_id}. "
    "Check that you entered the parent/master Run ID."
)

print("PASS: Parquet files were found for the selected batch.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Group files by schema and table</mark>

# CELL ********************

def extract_table_key(file_path):
    path_marker = "/raw/wwi/"

    if path_marker not in file_path:
        return None

    relative_path = file_path.split(path_marker, 1)[1]
    path_parts = relative_path.split("/")

    if len(path_parts) < 2:
        return None

    source_schema = path_parts[0]
    source_table = path_parts[1]

    return source_schema, source_table


files_by_table = defaultdict(list)

for file_path in batch_files:
    table_key = extract_table_key(file_path)

    if table_key is not None:
        files_by_table[table_key].append(file_path)


inventory_rows = []

for table_key, expected_rows in expected_counts.items():
    schema_name, table_name = table_key
    parquet_file_count = len(files_by_table.get(table_key, []))

    if parquet_file_count > 0:
        inventory_status = "FILES_PRESENT"
    elif expected_rows == 0:
        inventory_status = "EXPECTED_EMPTY_NO_FILE"
    else:
        inventory_status = "MISSING_FILES"

    inventory_rows.append({
        "SchemaName": schema_name,
        "TableName": table_name,
        "ExpectedRows": expected_rows,
        "ParquetFiles": parquet_file_count,
        "InventoryStatus": inventory_status
    })


inventory_df = spark.createDataFrame(inventory_rows)

display(
    inventory_df.orderBy("SchemaName", "TableName")
)

print(f"Expected table definitions : {len(expected_counts)}")
print(f"Tables with Parquet files  : {len(files_by_table)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Count rows in every Bronze table</mark>

# CELL ********************

def read_bronze_batch_table(schema_name, table_name):
    table_key = (schema_name, table_name)
    table_files = files_by_table.get(table_key, [])

    if not table_files:
        return None

    return spark.read.parquet(*table_files)


validation_rows = []

for table_key, expected_rows in expected_counts.items():
    schema_name, table_name = table_key
    table_files = files_by_table.get(table_key, [])

    try:
        if not table_files:
            actual_rows = 0
            column_count = 0

            if expected_rows == 0:
                status = "PASS_EMPTY"
                error_message = ""
            else:
                status = "FAIL_MISSING_FILES"
                error_message = "No Parquet files found"

        else:
            table_df = spark.read.parquet(*table_files)

            actual_rows = table_df.count()
            column_count = len(table_df.columns)

            if actual_rows == expected_rows:
                status = "PASS"
                error_message = ""
            else:
                status = "FAIL_ROW_COUNT"
                error_message = (
                    f"Expected {expected_rows}, found {actual_rows}"
                )

        difference = actual_rows - expected_rows

    except Exception as validation_error:
        actual_rows = None
        column_count = None
        difference = None
        status = "ERROR"
        error_message = str(validation_error)[:500]

    validation_rows.append({
        "SchemaName": schema_name,
        "TableName": table_name,
        "ExpectedRows": expected_rows,
        "ActualRows": actual_rows,
        "Difference": difference,
        "ParquetFiles": len(table_files),
        "ColumnCount": column_count,
        "Status": status,
        "ErrorMessage": error_message
    })


validation_df = spark.createDataFrame(validation_rows)

display(
    validation_df.orderBy("SchemaName", "TableName")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark> Produce the overall row-count summary</mark>

# CELL ********************

failed_tables = [
    row
    for row in validation_rows
    if not row["Status"].startswith("PASS")
]

unexpected_tables = sorted(
    set(files_by_table.keys()) - set(expected_counts.keys())
)

actual_total_rows = sum([
    row["ActualRows"]
    for row in validation_rows
    if row["ActualRows"] is not None
])

passed_table_count = sum([
    1
    for row in validation_rows
    if row["Status"].startswith("PASS")
])

print("BRONZE FULL-LOAD VALIDATION SUMMARY")
print("-----------------------------------")
print(f"Batch ID             : {p_batch_id}")
print(f"Expected tables       : {expected_table_count}")
print(f"Passed tables         : {passed_table_count}")
print(f"Failed tables         : {len(failed_tables)}")
print(f"Unexpected tables     : {len(unexpected_tables)}")
print(f"Expected total rows   : {expected_total_rows:,}")
print(f"Actual total rows     : {actual_total_rows:,}")
print(f"Total-row difference  : {actual_total_rows - expected_total_rows:,}")

if failed_tables:
    print("\nFailed-table details:")

    for failed in failed_tables:
        print(
            failed["SchemaName"],
            failed["TableName"],
            failed["Status"],
            failed["ErrorMessage"]
        )

if unexpected_tables:
    print("\nUnexpected tables:")

    for table_key in unexpected_tables:
        print(table_key)

assert not failed_tables, \
    "One or more Bronze tables failed validation."

assert not unexpected_tables, \
    f"Unexpected tables found: {unexpected_tables}"

assert actual_total_rows == expected_total_rows, \
    "Bronze total row count does not match the source baseline."

print("\nPASS: All Bronze row-count validations succeeded.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Validate special data types and columns</mark>

# CELL ********************

special_checks = []


def add_type_check(
    schema_name,
    table_name,
    column_name,
    expected_type
):
    try:
        table_df = read_bronze_batch_table(
            schema_name,
            table_name
        )

        if table_df is None:
            actual_type = "TABLE_NOT_AVAILABLE"
            status = "FAIL"

        elif column_name not in table_df.columns:
            actual_type = "COLUMN_NOT_FOUND"
            status = "FAIL"

        else:
            actual_type = (
                table_df.schema[column_name]
                .dataType
                .simpleString()
            )

            status = (
                "PASS"
                if actual_type == expected_type
                else "FAIL"
            )

    except Exception as check_error:
        actual_type = str(check_error)[:300]
        status = "ERROR"

    special_checks.append({
        "Check": f"{schema_name}.{table_name}.{column_name}",
        "Expected": expected_type,
        "Actual": actual_type,
        "Status": status
    })


# SQL geography was serialized to binary.
add_type_check(
    "Sales",
    "Customers",
    "DeliveryLocation",
    "binary"
)

# Temporal datetime2(7) values were converted to strings.
add_type_check(
    "Sales",
    "Customers",
    "ValidFrom",
    "string"
)

add_type_check(
    "Sales",
    "Customers",
    "ValidTo",
    "string"
)

add_type_check(
    "Sales",
    "Orders",
    "LastEditedWhen",
    "string"
)

# Binary product images must remain binary.
add_type_check(
    "Warehouse",
    "StockItems",
    "Photo",
    "binary"
)

# Computed SQL values should be present as ordinary raw values.
add_type_check(
    "Warehouse",
    "StockItems",
    "Tags",
    "string"
)

add_type_check(
    "Warehouse",
    "StockItems",
    "SearchDetails",
    "string"
)

add_type_check(
    "Warehouse",
    "StockItems",
    "ValidFrom",
    "string"
)

special_checks_df = spark.createDataFrame(special_checks)

display(special_checks_df)

failed_special_checks = [
    check
    for check in special_checks
    if check["Status"] != "PASS"
]

assert not failed_special_checks, (
    f"Special-column validation failed: "
    f"{failed_special_checks}"
)

print("PASS: Special Bronze column types are correct.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Validate seven-digit timestamp preservation</mark>

# CELL ********************

# Cell 9: Validate ISO timestamp strings
# Fractional seconds are optional but cannot exceed seven digits.

timestamp_pattern = (
    r"^\d{4}-\d{2}-\d{2}T"
    r"\d{2}:\d{2}:\d{2}"
    r"(\.\d{1,7})?$"
)

timestamp_checks = [
    ("Sales", "Customers", "ValidFrom"),
    ("Sales", "Customers", "ValidTo"),
    ("Sales", "Orders", "LastEditedWhen"),
    ("Warehouse", "StockItems", "ValidFrom"),
    ("Warehouse", "StockItems", "ValidTo")
]

timestamp_validation_rows = []

for schema_name, table_name, column_name in timestamp_checks:
    table_df = read_bronze_batch_table(
        schema_name,
        table_name
    )

    invalid_count = (
        table_df
        .filter(
            F.col(column_name).isNotNull()
            & ~F.col(column_name).rlike(timestamp_pattern)
        )
        .count()
    )

    status = "PASS" if invalid_count == 0 else "FAIL"

    timestamp_validation_rows.append({
        "TableName": f"{schema_name}.{table_name}",
        "ColumnName": column_name,
        "ExpectedFormat": (
            "yyyy-MM-ddTHH:mm:ss with optional 1–7 fractional digits"
        ),
        "InvalidRows": invalid_count,
        "Status": status
    })


timestamp_validation_df = spark.createDataFrame(
    timestamp_validation_rows
)

display(timestamp_validation_df)

failed_timestamp_checks = [
    row
    for row in timestamp_validation_rows
    if row["Status"] != "PASS"
]

assert not failed_timestamp_checks, (
    f"Timestamp validation failed: "
    f"{failed_timestamp_checks}"
)

print(
    "PASS: Timestamp values use valid ISO format "
    "with up to seven fractional digits."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <mark>Save the validation manifest</mark>

# CELL ********************

manifest_base_path = (
    "Files/manifests/wwi/"
    "full_load_validation/"
    f"batch_id={p_batch_id}/"
    f"validation_attempt_id={p_validation_attempt_id}"
)

table_manifest_path = (
    f"{manifest_base_path}/table_results"
)

special_checks_path = (
    f"{manifest_base_path}/special_column_checks"
)

timestamp_checks_path = (
    f"{manifest_base_path}/timestamp_checks"
)


table_manifest_df = (
    validation_df
    .withColumn("BatchID", F.lit(p_batch_id))
    .withColumn("LoadType", F.lit(p_load_type))
    .withColumn("IngestionAttemptID", F.lit(p_attempt_id))
    .withColumn(
        "ValidationAttemptID",
        F.lit(p_validation_attempt_id)
    )
    .withColumn(
        "ValidatedAtUTC",
        F.current_timestamp()
    )
)

table_manifest_df.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(table_manifest_path)

special_checks_df.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(special_checks_path)

timestamp_validation_df.coalesce(1).write \
    .mode("overwrite") \
    .format("parquet") \
    .save(timestamp_checks_path)

print("Validation manifests saved successfully.")
print(f"Manifest root: {manifest_base_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

overall_status = (
    "PASS"
    if (
        len(failed_tables) == 0
        and len(unexpected_tables) == 0
        and len(failed_special_checks) == 0
        and len(failed_timestamp_checks) == 0
        and actual_total_rows == expected_total_rows
    )
    else "FAIL"
)

validation_result = {
    "batch_id": p_batch_id,
    "load_type": p_load_type,
    "expected_tables": expected_table_count,
    "passed_tables": passed_table_count,
    "failed_tables": len(failed_tables),
    "expected_rows": expected_total_rows,
    "actual_rows": actual_total_rows,
    "overall_status": overall_status,
    "manifest_path": manifest_base_path
}

print(
    json.dumps(
        validation_result,
        indent=2
    )
)

assert overall_status == "PASS", \
    "The Bronze batch is not ready for downstream processing."

print("\nBRONZE FULL LOAD IS VALIDATED.")

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
