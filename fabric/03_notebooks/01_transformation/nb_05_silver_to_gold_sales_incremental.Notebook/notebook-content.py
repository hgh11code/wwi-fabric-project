# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bf2d1df8-0367-4d71-bcdb-66140e5aad8b",
# META       "default_lakehouse_name": "wwi_gold",
# META       "default_lakehouse_workspace_id": "c64ac1e1-7c4e-48d9-a80e-89d584a3b1c0",
# META       "known_lakehouses": [
# META         {
# META           "id": "efe3c3ab-bf7f-4683-bdab-5a6cb3a057da"
# META         },
# META         {
# META           "id": "bf2d1df8-0367-4d71-bcdb-66140e5aad8b"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Pipeline parameters

p_silver_run_id = ""
p_bronze_batch_id = ""
p_pipeline_exit = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark import StorageLevel
from delta.tables import DeltaTable
from datetime import datetime, timezone, date
from decimal import Decimal
from functools import reduce
import json, uuid

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.ansi.enabled", "true")
GOLD_RUN_ID = str(uuid.uuid4())
PROCESSED_AT_UTC = datetime.now(timezone.utc)
SILVER = "wwi_silver"
GOLD = "wwi_gold"
STATE = {"silver_run_id": str(p_silver_run_id or "").strip() or None,
         "bronze_batch_id": None, "stage": "INITIALIZATION",
         "table": "__RUN__", "completed_tables": 0, "fact_rows": None}
CHECKS = []
HELD = []
RUN_COMPLETED = False
GOLD_RESULT = None
print("Gold RunID:", GOLD_RUN_ID)
SILVER_FILES_ROOT = (
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

LOG_SCHEMAS = {
 "gold_run_log": "gold_run_id STRING, silver_run_id STRING, bronze_batch_id STRING, model_name STRING, load_type STRING, run_status STRING, expected_table_count INT, completed_table_count INT, fact_row_count BIGINT, started_at_utc TIMESTAMP, completed_at_utc TIMESTAMP, error_message STRING",
 "gold_input_versions": "gold_run_id STRING, silver_run_id STRING, bronze_batch_id STRING, source_table STRING, delta_table_id STRING, delta_version BIGINT, delta_location STRING, expected_row_count BIGINT, actual_row_count BIGINT, checked_at_utc TIMESTAMP",
 "gold_validation_results": "gold_run_id STRING, table_name STRING, check_name STRING, expected_value STRING, actual_value STRING, check_status STRING, checked_at_utc TIMESTAMP",
 "gold_table_results": "gold_run_id STRING, table_name STRING, table_status STRING, expected_row_count BIGINT, actual_row_count BIGINT, completed_at_utc TIMESTAMP",
 "gold_failures": "gold_run_id STRING, silver_run_id STRING, processing_stage STRING, table_name STRING, error_type STRING, error_message STRING, failed_at_utc TIMESTAMP"
}
for name, ddl in LOG_SCHEMAS.items():
    spark.sql(f"CREATE TABLE IF NOT EXISTS {GOLD}.common.{name} ({ddl}) USING DELTA")
print("Gold operational tables ready.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def utc_now():
    return datetime.now(timezone.utc)

def log_append(name, records):
    if not records: return
    target = f"{GOLD}.common.{name}"
    df = spark.createDataFrame(records, schema=spark.table(target).schema)
    df.write.format("delta").mode("append").saveAsTable(target)

def save_run(status, error=None):
    target = f"{GOLD}.common.gold_run_log"
    row = {"gold_run_id": GOLD_RUN_ID, "silver_run_id": STATE["silver_run_id"],
           "bronze_batch_id": STATE["bronze_batch_id"], "model_name": "wwi_sales",
           "load_type": "INCREMENTAL_REFRESH", "run_status": status, "expected_table_count": 4,
           "completed_table_count": STATE["completed_tables"], "fact_row_count": STATE["fact_rows"],
           "started_at_utc": PROCESSED_AT_UTC,
           "completed_at_utc": utc_now() if status in ("COMPLETED", "FAILED") else None,
           "error_message": str(error)[:8000] if error else None}
    df = spark.createDataFrame([row], schema=spark.table(target).schema)
    (DeltaTable.forName(spark, target).alias("t")
     .merge(df.alias("s"), "t.gold_run_id = s.gold_run_id")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

def check(table, name, actual, expected):
    passed = actual == expected
    CHECKS.append({"gold_run_id": GOLD_RUN_ID, "table_name": table,
                   "check_name": name, "expected_value": str(expected), "actual_value": str(actual),
                   "check_status": "PASSED" if passed else "FAILED", "checked_at_utc": utc_now()})
    if not passed: raise ValueError(f"{table}: {name}; expected {expected}, got {actual}")

def flush_checks():
    log_append("gold_validation_results", CHECKS)
    CHECKS.clear()

def invalid_count(df, condition):
    return int(df.agg(F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0)).alias("n")).first()["n"])

def require_not_null(df, columns, table):
    condition = reduce(lambda a,b:a|b, (F.col(c).isNull() for c in columns))
    check(table, "REQUIRED_VALUES:" + ",".join(columns), invalid_count(df, condition), 0)

def unique_key(df, columns, table):
    require_not_null(df, columns, table)
    groups = df.groupBy(*columns).count().filter(F.col("count") > 1).count()
    check(table, "DUPLICATE_KEY_GROUPS:" + ",".join(columns), groups, 0)

def foreign_key(child, child_col, parent, parent_col, label, nullable=False):
    if not nullable: require_not_null(child, [child_col], label)
    missing = (child.filter(F.col(child_col).isNotNull()).select(F.col(child_col).alias("fk"))
               .join(parent.select(F.col(parent_col).alias("fk")), "fk", "left_anti").count())
    check(label, "UNMATCHED:"+child_col, missing, 0)

def hold(df):
    df.persist(StorageLevel.MEMORY_AND_DISK)
    HELD.append(df)
    return df

def add_lineage(df):
    return (df.withColumn("_gold_run_id", F.lit(GOLD_RUN_ID))
            .withColumn("_silver_run_id", F.lit(STATE["silver_run_id"]))
            .withColumn("_bronze_batch_id", F.lit(STATE["bronze_batch_id"]))
            .withColumn("_processed_at_utc", F.lit(PROCESSED_AT_UTC).cast("timestamp")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

CONTRACTS = {
 "sales.invoices": {"invoice_id":"int", "customer_id":"int", "bill_to_customer_id":"int", "order_id":"int", "invoice_date":"date", "is_credit_note":"boolean"},
 "sales.invoice_lines": {"invoice_line_id":"int", "invoice_id":"int", "stock_item_id":"int", "quantity":"int", "unit_price":"decimal(18,2)", "tax_rate":"decimal(18,3)", "tax_amount":"decimal(18,2)", "line_profit":"decimal(18,2)", "extended_price":"decimal(18,2)"},
 "sales.customers": {"customer_id":"int", "customer_name":"string", "customer_category_id":"int", "delivery_city_id":"int", "account_opened_date":"date", "is_on_credit_hold":"boolean", "payment_days":"int"},
 "sales.customer_categories": {"customer_category_id":"int", "customer_category_name":"string"},
 "reference.cities": {"city_id":"int", "city_name":"string", "state_province_id":"int"},
 "reference.state_provinces": {"state_province_id":"int", "state_province_name":"string", "state_province_code":"string", "country_id":"int", "sales_territory":"string"},
 "reference.countries": {"country_id":"int", "country_name":"string", "iso_alpha3_code":"string", "continent":"string", "region":"string", "subregion":"string"},
 "inventory.stock_items": {"stock_item_id":"int", "stock_item_name":"string", "supplier_id":"int", "color_id":"int", "unit_package_id":"int", "outer_package_id":"int", "brand":"string", "size":"string", "quantity_per_outer":"int", "is_chiller_stock":"boolean", "lead_time_days":"int"},
 "inventory.colors": {"color_id":"int", "color_name":"string"},
 "inventory.package_types": {"package_type_id":"int", "package_type_name":"string"}
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SOURCE_KEYS = {
    "sales.invoices": "invoice_id",
    "sales.invoice_lines": "invoice_line_id",
    "sales.customers": "customer_id",
    "sales.customer_categories": "customer_category_id",
    "reference.cities": "city_id",
    "reference.state_provinces": "state_province_id",
    "reference.countries": "country_id",
    "inventory.stock_items": "stock_item_id",
    "inventory.colors": "color_id",
    "inventory.package_types": "package_type_id"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_silver_success_manifest(path):
    full_path = (
        path
        if str(path).startswith("abfss://")
        else f"{SILVER_FILES_ROOT}/{str(path).lstrip('/')}"
    )

    if not notebookutils.fs.exists(full_path):
        raise RuntimeError(
            f"Silver success manifest does not exist: {full_path}"
        )

    text = notebookutils.fs.head(
        full_path, 1024 * 1024
    ).lstrip("\ufeff").strip()

    if not text:
        raise RuntimeError("Silver success manifest is empty")

    return json.loads(text), full_path

def select_silver_incremental_run():
    runs = spark.table(f"{SILVER}.ops.silver_batch_log")
    completed = runs.filter(F.upper(F.col("BatchStatus")) == F.lit("COMPLETED"))
    requested_silver_run = str(p_silver_run_id or "").strip()
    requested_bronze_batch = str(p_bronze_batch_id or "").strip()
    if not requested_silver_run or not requested_bronze_batch:
        raise ValueError("Both p_silver_run_id and p_bronze_batch_id are required.")

    rows = (completed
        .filter(F.col("SilverRunID") == requested_silver_run)
        .filter(F.col("BronzeBatchID") == requested_bronze_batch)
        .collect())
    if len(rows) != 1:
        raise RuntimeError(
            "Expected exactly one completed Silver record for the supplied "
            f"IDs; found {len(rows)}."
        )
    selected = rows[0]

    newer_completed = (completed
        .filter(F.to_timestamp("CompletedAtUTC") >
                F.to_timestamp(F.lit(selected["CompletedAtUTC"])))
        .limit(1).count())
    if newer_completed:
        raise RuntimeError(
            "A newer completed Silver run exists. Gold will not label the "
            "current Silver state with an older run ID."
        )

    success_manifest, success_manifest_full_path = read_silver_success_manifest(
        selected["SuccessManifestPath"]
    )
    expected_manifest = {
        "BronzeBatchID": requested_bronze_batch,
        "BronzeAttemptID": str(selected["BronzeAttemptID"]),
        "SilverRunID": requested_silver_run,
        "Status": "COMPLETED",
        "ProcessedTableCount": 48,
        "ValidatedTableCount": 48,
        "FailedTableCount": 0,
        "ProcessManifestPath": selected["ProcessManifestPath"],
        "ValidationManifestPath": selected["ValidationManifestPath"],
        "CompletedAtUTC": selected["CompletedAtUTC"]
    }
    for field, expected_value in expected_manifest.items():
        actual_value = success_manifest.get(field)
        if str(actual_value) != str(expected_value):
            raise RuntimeError(
                f"Silver success manifest {field} mismatch: "
                f"expected {expected_value!r}, got {actual_value!r}"
            )

    STATE["silver_run_id"] = selected["SilverRunID"]
    STATE["bronze_batch_id"] = selected["BronzeBatchID"]
    check("__RUN__", "SILVER_BATCH_STATUS", selected["BatchStatus"], "COMPLETED")
    check("__RUN__", "SILVER_PROCESSED_TABLES", selected["ProcessedTableCount"], 48)
    check("__RUN__", "SILVER_VALIDATED_TABLES", selected["ValidatedTableCount"], 48)
    check("__RUN__", "SILVER_FAILED_TABLES", selected["FailedTableCount"], 0)

    if not selected["SuccessManifestPath"]:
        raise RuntimeError("The selected Silver batch has no success-manifest path.")
    print("Selected Silver incremental batch")
    print("Silver Run ID:", STATE["silver_run_id"])
    print("Bronze Batch ID:", STATE["bronze_batch_id"])
    print("Completed At:", selected["CompletedAtUTC"])
    print("Success Manifest:", success_manifest_full_path)
    return selected.asDict()


SILVER_RUN = select_silver_incremental_run()
def select_silver_incremental_run():
    runs = spark.table(f"{SILVER}.ops.silver_batch_log")

    completed = runs.filter(
        F.upper(F.col("BatchStatus")) == F.lit("COMPLETED")
    )

    requested_silver_run = str(p_silver_run_id or "").strip()
    requested_bronze_batch = str(p_bronze_batch_id or "").strip()

    # Pipeline can explicitly provide the Silver run.
    if requested_silver_run:
        candidates = completed.filter(
            F.col("SilverRunID") == requested_silver_run
        )

    # Alternatively, select the completed Silver run for a Bronze batch.
    elif requested_bronze_batch:
        candidates = completed.filter(
            F.col("BronzeBatchID") == requested_bronze_batch
        )

    # Manual execution: use the latest completed Silver batch.
    else:
        candidates = completed

    rows = (
        candidates
        .orderBy(
            F.to_timestamp("CompletedAtUTC").desc_nulls_last()
        )
        .limit(2)
        .collect()
    )

    if not rows:
        raise RuntimeError(
            "No completed Silver incremental batch matched the supplied parameters."
        )

    selected = rows[0]

    # Prevent ambiguous selection if completion timestamps are identical.
    if (
        not requested_silver_run
        and len(rows) > 1
        and rows[0]["CompletedAtUTC"] == rows[1]["CompletedAtUTC"]
    ):
        raise RuntimeError(
            "Multiple Silver runs have the same completion time. "
            "Supply p_silver_run_id explicitly."
        )

    STATE["silver_run_id"] = selected["SilverRunID"]
    STATE["bronze_batch_id"] = selected["BronzeBatchID"]

    check(
        "__RUN__",
        "SILVER_BATCH_STATUS",
        selected["BatchStatus"],
        "COMPLETED"
    )

    check(
        "__RUN__",
        "SILVER_PROCESSED_TABLES",
        selected["ProcessedTableCount"],
        48
    )

    check(
        "__RUN__",
        "SILVER_VALIDATED_TABLES",
        selected["ValidatedTableCount"],
        48
    )

    check(
        "__RUN__",
        "SILVER_FAILED_TABLES",
        selected["FailedTableCount"],
        0
    )

    if not selected["SuccessManifestPath"]:
        raise RuntimeError(
            "The selected Silver batch has no success-manifest path."
        )

    print("Selected Silver incremental batch")
    print("Silver Run ID:", STATE["silver_run_id"])
    print("Bronze Batch ID:", STATE["bronze_batch_id"])
    print("Completed At:", selected["CompletedAtUTC"])
    print("Success Manifest:", selected["SuccessManifestPath"])

    return selected.asDict()


SILVER_RUN = select_silver_incremental_run()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_current_silver_inputs():
    inputs = {}
    current_counts = {}

    for table, contract in CONTRACTS.items():
        STATE["table"] = table
        full_name = f"{SILVER}.{table}"

        # Capture the exact current Delta version.
        delta_table = DeltaTable.forName(spark, full_name)

        detail = (
            delta_table.detail()
            .select("id", "location")
            .first()
        )

        version = int(
            delta_table.history(1)
            .select("version")
            .first()["version"]
        )

        # Read a fixed Delta version so the Gold run remains consistent.
        df = (
            spark.read
            .format("delta")
            .option("versionAsOf", version)
            .load(detail["location"])
        )

        actual_types = {
            field.name: field.dataType.simpleString()
            for field in df.schema.fields
        }

        required_columns = {
            **contract,
            "_silver_run_id": "string",
            "_batch_id": "string"
        }

        schema_differences = [
            f"{column}: expected {expected_type}, "
            f"got {actual_types.get(column)}"
            for column, expected_type in required_columns.items()
            if actual_types.get(column) != expected_type
        ]

        check(
            table,
            "SCHEMA_CONTRACT",
            schema_differences,
            []
        )

        df = hold(df.select(*required_columns.keys()))

        metrics = (
            df.agg(
                F.count("*").alias("total_rows"),

                F.coalesce(
                    F.sum(
                        F.when(
                            F.col("_silver_run_id")
                            == F.lit(STATE["silver_run_id"]),
                            1
                        ).otherwise(0)
                    ),
                    F.lit(0)
                ).alias("rows_from_selected_silver_run"),

                F.coalesce(
                    F.sum(
                        F.when(
                            F.col("_batch_id")
                            == F.lit(STATE["bronze_batch_id"]),
                            1
                        ).otherwise(0)
                    ),
                    F.lit(0)
                ).alias("rows_from_selected_bronze_batch")
            )
            .first()
        )

        total_rows = int(metrics["total_rows"])
        silver_run_rows = int(metrics["rows_from_selected_silver_run"])
        bronze_batch_rows = int(metrics["rows_from_selected_bronze_batch"])

        if total_rows <= 0:
            raise RuntimeError(
                f"Required Gold input table is empty: {full_name}"
            )

        # Verify that the complete current Silver table has unique keys.
        unique_key(
            df,
            [SOURCE_KEYS[table]],
            table
        )

        current_counts[table] = total_rows

        log_append(
            "gold_input_versions",
            [{
                "gold_run_id": GOLD_RUN_ID,
                "silver_run_id": STATE["silver_run_id"],
                "bronze_batch_id": STATE["bronze_batch_id"],
                "source_table": full_name,
                "delta_table_id": detail["id"],
                "delta_version": version,
                "delta_location": detail["location"],
                "expected_row_count": total_rows,
                "actual_row_count": total_rows,
                "checked_at_utc": utc_now()
            }]
        )

        inputs[table] = df

        print(
            f"INPUT PASSED: {table} | "
            f"total={total_rows:,} | "
            f"latest Silver run rows={silver_run_rows:,} | "
            f"latest Bronze batch rows={bronze_batch_rows:,} | "
            f"Delta version={version}"
        )

    return inputs, current_counts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_date(inputs):
    invoices = inputs["sales.invoices"]
    require_not_null(invoices, ["invoice_date"], "sales.invoices")
    bounds = invoices.agg(F.min("invoice_date").alias("lo"),F.max("invoice_date").alias("hi")).first()
    start, end = date(bounds["lo"].year,1,1), date(bounds["hi"].year,12,31)
    df = (spark.range(1).select(F.explode(F.sequence(F.lit(start),F.lit(end),F.expr("INTERVAL 1 DAY"))).alias("calendar_date"))
       .withColumn("date_key",F.date_format("calendar_date","yyyyMMdd").cast("int"))
       .withColumn("calendar_year",F.year("calendar_date"))
       .withColumn("calendar_quarter",F.quarter("calendar_date"))
       .withColumn("month_number",F.month("calendar_date"))
       .withColumn("month_name",F.date_format("calendar_date","MMMM"))
       .withColumn("year_month",F.date_format("calendar_date","yyyy-MM"))
       .withColumn("year_month_key",F.year("calendar_date")*100+F.month("calendar_date"))
       .withColumn("day_of_month",F.dayofmonth("calendar_date"))
       .withColumn("day_name",F.date_format("calendar_date","EEEE"))
       .withColumn("day_of_week_number",F.pmod(F.dayofweek("calendar_date")+5,F.lit(7))+1)
       .withColumn("is_weekend",F.col("day_of_week_number")>=6)
       .withColumn("month_start_date",F.trunc("calendar_date","month"))
       .withColumn("month_end_date",F.last_day("calendar_date"))
       .select("date_key","calendar_date","calendar_year","calendar_quarter","month_number","month_name",
               "year_month","year_month_key","day_of_month","day_name","day_of_week_number","is_weekend","month_start_date","month_end_date"))
    foreign_key(invoices,"invoice_date",df,"calendar_date","common.dim_date")
    return df, (end-start).days+1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def build_customer(inputs):
    c=inputs["sales.customers"]; cat=inputs["sales.customer_categories"]
    city=inputs["reference.cities"]; state=inputs["reference.state_provinces"]; country=inputs["reference.countries"]
    foreign_key(c,"customer_category_id",cat,"customer_category_id","sales.dim_customer/category")
    foreign_key(c,"delivery_city_id",city,"city_id","sales.dim_customer/city")
    foreign_key(city,"state_province_id",state,"state_province_id","sales.dim_customer/state")
    foreign_key(state,"country_id",country,"country_id","sales.dim_customer/country")
    df=(c.alias("c").join(cat.alias("cat"),F.col("c.customer_category_id")==F.col("cat.customer_category_id"),"left")
        .join(city.alias("city"),F.col("c.delivery_city_id")==F.col("city.city_id"),"left")
        .join(state.alias("s"),F.col("city.state_province_id")==F.col("s.state_province_id"),"left")
        .join(country.alias("co"),F.col("s.country_id")==F.col("co.country_id"),"left")
        .select(F.col("c.customer_id").alias("customer_key"),F.col("c.customer_id"),F.col("c.customer_name"),
                F.col("cat.customer_category_name"),F.col("c.delivery_city_id"),
                F.col("city.city_name").alias("delivery_city_name"),F.col("s.state_province_id"),
                F.col("s.state_province_code"),F.col("s.state_province_name"),F.col("s.sales_territory"),
                F.col("co.country_id"),F.col("co.country_name"),F.col("co.iso_alpha3_code"),
                F.col("co.continent"),F.col("co.region"),F.col("co.subregion"),
                F.col("c.account_opened_date"),F.col("c.payment_days"),F.col("c.is_on_credit_hold")))
    require_not_null(df,["customer_name","customer_category_name","delivery_city_name","state_province_name","country_name"],"sales.dim_customer")
    return df

def build_product(inputs):
    s=inputs["inventory.stock_items"]; colors=inputs["inventory.colors"]; packages=inputs["inventory.package_types"]
    foreign_key(s,"color_id",colors,"color_id","inventory.dim_stock_item/color",nullable=True)
    foreign_key(s,"unit_package_id",packages,"package_type_id","inventory.dim_stock_item/unit_package")
    foreign_key(s,"outer_package_id",packages,"package_type_id","inventory.dim_stock_item/outer_package")
    df=(s.alias("s").join(colors.alias("c"),F.col("s.color_id")==F.col("c.color_id"),"left")
        .join(packages.alias("u"),F.col("s.unit_package_id")==F.col("u.package_type_id"),"left")
        .join(packages.alias("o"),F.col("s.outer_package_id")==F.col("o.package_type_id"),"left")
        .select(F.col("s.stock_item_id").alias("stock_item_key"),F.col("s.stock_item_id"),F.col("s.stock_item_name"),
                F.col("s.supplier_id"),F.col("s.color_id"),F.col("c.color_name"),F.col("s.brand"),F.col("s.size"),
                F.col("s.unit_package_id"),F.col("u.package_type_name").alias("unit_package_name"),
                F.col("s.outer_package_id"),F.col("o.package_type_name").alias("outer_package_name"),
                F.col("s.quantity_per_outer"),F.col("s.is_chiller_stock"),F.col("s.lead_time_days")))
    require_not_null(df,["stock_item_name","unit_package_name","outer_package_name"],"inventory.dim_stock_item")
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

MEASURES = ["quantity","sales_amount_excl_tax","tax_amount","sales_amount_incl_tax","line_profit"]

def money_totals(df):
    return df.agg(*[F.sum(c).alias(c) for c in MEASURES]).first().asDict()

def reconcile_fact(actual, expected, label):
    totals=money_totals(actual)
    for name,value in expected.items(): check(label,"TOTAL:"+name,totals[name],value)

def build_fact(inputs, dates, customers, products):
    lines=inputs["sales.invoice_lines"]; headers=inputs["sales.invoices"]
    require_not_null(headers,["customer_id","bill_to_customer_id","invoice_date","is_credit_note"],"sales.invoices")
    check("sales.invoices","CREDIT_NOTES_REQUIRE_SEPARATE_POLICY",invalid_count(headers,F.col("is_credit_note")==True),0)
    require_not_null(lines,["invoice_id","stock_item_id","quantity","unit_price","tax_rate","tax_amount","line_profit","extended_price"],"sales.invoice_lines")
    check("sales.invoice_lines","REGULAR_INVOICE_SIGN_POLICY",invalid_count(lines,
        (F.col("quantity")<=0)|(F.col("unit_price")<0)|(F.col("tax_rate")<0)|(F.col("tax_amount")<0)|(F.col("extended_price")<0)),0)
    foreign_key(lines,"invoice_id",headers,"invoice_id","sales.fact_invoice_line/header")
    measured=(lines.withColumn("sales_amount_excl_tax",(F.col("extended_price")-F.col("tax_amount")).cast("decimal(28,2)"))
              .withColumn("sales_amount_incl_tax",F.col("extended_price").cast("decimal(28,2)")))
    check("sales.invoice_lines","EXTENDED_PRICE_FORMULA",invalid_count(measured,
          ~F.col("sales_amount_excl_tax").eqNullSafe((F.col("quantity")*F.col("unit_price")).cast("decimal(28,2)"))),0)
    baseline=money_totals(measured)
    fact=(measured.alias("l").join(headers.alias("h"),F.col("l.invoice_id")==F.col("h.invoice_id"),"inner")
          .select(F.col("l.invoice_line_id"),F.col("l.invoice_id"),F.col("h.order_id"),
                  F.date_format(F.col("h.invoice_date"),"yyyyMMdd").cast("int").alias("date_key"),
                  F.col("h.invoice_date"),F.col("h.customer_id").alias("customer_key"),
                  F.col("h.bill_to_customer_id").alias("bill_to_customer_key"),
                  F.col("l.stock_item_id").alias("stock_item_key"),F.col("h.is_credit_note"),
                  F.col("l.quantity"),F.col("l.unit_price"),F.col("l.tax_rate"),
                  F.col("l.sales_amount_excl_tax"),F.col("l.tax_amount"),
                  F.col("l.sales_amount_incl_tax"),F.col("l.line_profit")))
    foreign_key(fact,"date_key",dates,"date_key","sales.fact_invoice_line/date")
    foreign_key(fact,"customer_key",customers,"customer_key","sales.fact_invoice_line/customer")
    foreign_key(fact,"bill_to_customer_key",customers,"customer_key","sales.fact_invoice_line/bill_to_customer")
    foreign_key(fact,"stock_item_key",products,"stock_item_key","sales.fact_invoice_line/product")
    reconcile_fact(fact,baseline,"sales.fact_invoice_line")
    return fact,baseline

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

STATE["stage"] = "SILVER_INPUT_VERIFICATION"
STATE["table"] = "__RUN__"

# Read the complete current Silver state at fixed Delta versions.
inputs, current_counts = read_current_silver_inputs()

flush_checks()

# Build the Gold date dimension.
STATE["stage"] = "BUILD_DATE"
STATE["table"] = "common.dim_date"

dates, date_count = build_date(inputs)

# Build the Gold customer dimension.
STATE["stage"] = "BUILD_CUSTOMER"
STATE["table"] = "sales.dim_customer"

customers = build_customer(inputs)

# Build the Gold stock-item dimension.
STATE["stage"] = "BUILD_PRODUCT"
STATE["table"] = "inventory.dim_stock_item"

products = build_product(inputs)

# Build the Gold invoice-line fact.
STATE["stage"] = "BUILD_FACT"
STATE["table"] = "sales.fact_invoice_line"

fact, baseline = build_fact(
    inputs,
    dates,
    customers,
    products
)

# Add Gold and upstream lineage columns.
outputs = {
    "common.dim_date": hold(add_lineage(dates)),
    "sales.dim_customer": hold(add_lineage(customers)),
    "inventory.dim_stock_item": hold(add_lineage(products)),
    "sales.fact_invoice_line": hold(add_lineage(fact))
}

# Expected Gold counts come from the complete current Silver state.
expected = {
    "common.dim_date": date_count,
    "sales.dim_customer": current_counts["sales.customers"],
    "inventory.dim_stock_item": current_counts["inventory.stock_items"],
    "sales.fact_invoice_line": current_counts["sales.invoice_lines"]
}

print("Gold candidate DataFrames built successfully")
print("Expected Gold row counts:")

for table, row_count in expected.items():
    print(f"  {table}: {row_count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

OUTPUT_KEYS = {
    "common.dim_date": "date_key",
    "sales.dim_customer": "customer_key",
    "inventory.dim_stock_item": "stock_item_key",
    "sales.fact_invoice_line": "invoice_line_id"
}


def validate_output(table, df, expected_rows):
    actual_rows = df.count()

    check(
        table,
        "OUTPUT_ROW_COUNT",
        actual_rows,
        expected_rows
    )

    unique_key(
        df,
        [OUTPUT_KEYS[table]],
        table
    )

    if table == "common.dim_date":
        unique_key(
            df,
            ["calendar_date"],
            table
        )

    expected_lineage = {
        "_gold_run_id": GOLD_RUN_ID,
        "_silver_run_id": STATE["silver_run_id"],
        "_bronze_batch_id": STATE["bronze_batch_id"]
    }

    invalid_lineage_condition = reduce(
        lambda left, right: left | right,
        (
            ~F.col(column).eqNullSafe(F.lit(value))
            for column, value in expected_lineage.items()
        )
    )

    invalid_lineage_condition = (
        invalid_lineage_condition
        | F.col("_processed_at_utc").isNull()
    )

    check(
        table,
        "OUTPUT_LINEAGE",
        invalid_count(df, invalid_lineage_condition),
        0
    )


def publish(table, df, expected_rows):
    STATE["table"] = table
    STATE["stage"] = "GOLD_WRITE"

    target = f"{GOLD}.{table}"

    # Intentional current-state rebuild of the small Gold model.
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target)
    )

    STATE["stage"] = "POST_WRITE_VALIDATION"

    written = spark.table(target)

    expected_schema = [
        (field.name, field.dataType.simpleString())
        for field in df.schema.fields
    ]

    written_schema = [
        (field.name, field.dataType.simpleString())
        for field in written.schema.fields
    ]

    check(
        table,
        "WRITTEN_SCHEMA",
        written_schema,
        expected_schema
    )

    validate_output(
        table,
        written,
        expected_rows
    )

    # Compare in both directions to verify exact published content.
    check(
        table,
        "MISSING_WRITTEN_CONTENT",
        df.exceptAll(
            written.select(df.columns)
        ).limit(1).count(),
        0
    )

    check(
        table,
        "EXTRA_WRITTEN_CONTENT",
        written.select(df.columns)
        .exceptAll(df)
        .limit(1)
        .count(),
        0
    )

    log_append(
        "gold_table_results",
        [{
            "gold_run_id": GOLD_RUN_ID,
            "table_name": table,
            "table_status": "VALIDATED",
            "expected_row_count": expected_rows,
            "actual_row_count": expected_rows,
            "completed_at_utc": utc_now()
        }]
    )

    STATE["completed_tables"] += 1

    flush_checks()
    save_run("RUNNING")

    print(
        f"GOLD VERIFIED: {target} "
        f"({expected_rows:,} rows)"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    STATE["completed_tables"] = 0
    STATE["fact_rows"] = None

    save_run("RUNNING")

    # ---------------------------------------------------------
    # Pre-write validation
    # ---------------------------------------------------------
    STATE["stage"] = "PRE_WRITE_VALIDATION"

    for table, df in outputs.items():
        STATE["table"] = table

        validate_output(
            table,
            df,
            expected[table]
        )

    flush_checks()

    print("All four Gold outputs validated before writing.")

    # ---------------------------------------------------------
    # Publish the four Gold tables
    # ---------------------------------------------------------
    for table, df in outputs.items():
        publish(
            table,
            df,
            expected[table]
        )

    # ---------------------------------------------------------
    # Final validation using the physically written tables
    # ---------------------------------------------------------
    STATE["stage"] = "FINAL_RECONCILIATION"
    STATE["table"] = "__RUN__"

    check(
        "__RUN__",
        "COMPLETED_TABLES",
        STATE["completed_tables"],
        4
    )

    current_gold = {
        table: spark.table(f"{GOLD}.{table}")
        for table in outputs
    }

    for table, df in current_gold.items():
        validate_output(
            table,
            df,
            expected[table]
        )

    gold_fact = current_gold["sales.fact_invoice_line"]

    foreign_key(
        gold_fact,
        "date_key",
        current_gold["common.dim_date"],
        "date_key",
        "FINAL/date"
    )

    foreign_key(
        gold_fact,
        "customer_key",
        current_gold["sales.dim_customer"],
        "customer_key",
        "FINAL/customer"
    )

    foreign_key(
        gold_fact,
        "bill_to_customer_key",
        current_gold["sales.dim_customer"],
        "customer_key",
        "FINAL/bill_to_customer"
    )

    foreign_key(
        gold_fact,
        "stock_item_key",
        current_gold["inventory.dim_stock_item"],
        "stock_item_key",
        "FINAL/product"
    )

    reconcile_fact(
        gold_fact,
        baseline,
        "FINAL/fact"
    )

    flush_checks()

    STATE["fact_rows"] = expected["sales.fact_invoice_line"]

    save_run("COMPLETED")

    RUN_COMPLETED = True

    GOLD_RESULT = {
        "status": "COMPLETED",
        "load_type": "INCREMENTAL_REFRESH",
        "gold_run_id": GOLD_RUN_ID,
        "silver_run_id": STATE["silver_run_id"],
        "bronze_batch_id": STATE["bronze_batch_id"],
        "tables": 4,
        "row_counts": expected,
        "sales_totals": {
            key: str(value)
            for key, value in baseline.items()
        }
    }

    print(json.dumps(GOLD_RESULT, indent=2))

except Exception as error:
    RUN_COMPLETED = False

    failure = {
        "gold_run_id": GOLD_RUN_ID,
        "silver_run_id": STATE["silver_run_id"],
        "processing_stage": STATE["stage"],
        "table_name": STATE["table"],
        "error_type": type(error).__name__,
        "error_message": str(error)[:8000],
        "failed_at_utc": utc_now()
    }

    logging_actions = [
        flush_checks,
        lambda: log_append("gold_failures", [failure]),
        lambda: save_run("FAILED", error)
    ]

    for action in logging_actions:
        try:
            action()
        except Exception as logging_error:
            print(
                "Quality logging also failed:",
                str(logging_error)[:500]
            )

    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not RUN_COMPLETED or GOLD_RESULT is None:
    raise RuntimeError(
        "Gold refresh did not complete. "
        "Resolve the failure and rerun the notebook in order."
    )

# Release cached Silver inputs and Gold outputs.
for frame in HELD:
    try:
        frame.unpersist()
    except Exception as cleanup_error:
        print(
            "Cache cleanup warning:",
            str(cleanup_error)[:200]
        )

HELD.clear()

if str(p_pipeline_exit).lower() == "true":
    notebookutils.notebook.exit(
        json.dumps(GOLD_RESULT)
    )
else:
    print(
        "Gold incremental refresh completed successfully."
    )
    print("Gold Run ID:", GOLD_RUN_ID)

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
