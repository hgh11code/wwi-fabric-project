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
# META           "id": "bf2d1df8-0367-4d71-bcdb-66140e5aad8b"
# META         },
# META         {
# META           "id": "efe3c3ab-bf7f-4683-bdab-5a6cb3a057da"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # WWI: Silver to Gold — sales full load
# 
# ## Run this notebook
# 1. Import into `03_transformation`; attach **wwi_gold as default** and **wwi_silver additionally**.
# 2. Run all 12 code cells in order in a fresh Fabric Spark session. No previous notebook variables are required.
# 3. Cell 1 is the parameter cell. Leave `p_silver_run_id` blank to use the latest completed Silver run, or supply its exact ID. In a pipeline pass the Silver notebook's returned RunID and set `p_pipeline_exit=True`.
# 4. Cells 3 and 11 write Gold only. Cell 3 creates missing operational tables in the existing `common` schema. Cell 11 validates all four proposed business tables **before** overwriting any business table, then verifies the written results.
# 
# ## Four business tables
# | Table | Grain / key | Purpose |
# |---|---|---|
# | `common.dim_date` | One calendar date / `date_key` (YYYYMMDD) | Complete calendar years covering invoices |
# | `sales.dim_customer` | One current customer / `customer_key` | Category and current delivery geography |
# | `inventory.dim_stock_item` | One current stock item / `stock_item_key` | Product attributes and unit/outer package labels |
# | `sales.fact_invoice_line` | One invoice line / `invoice_line_id` | Sales, tax, quantity and recorded profit |
# 
# This is a **full rebuild**, not incremental processing. Dimensions use current attributes (Type 1); historical invoices are classified using current customer/product attributes. Customer and stock-item dimension keys deliberately equal their source IDs for this single-source model; they are not generated SCD2 surrogate keys. Future multiple sources/SCD2 require a key migration.
# 
# Sales excluding tax = `extended_price - tax_amount`; independently checked against `quantity * unit_price`. Sales including tax = source `extended_price`. Profit = recorded `line_profit`, not net profit after overhead. Profit margin in reporting = SUM(line_profit) / SUM(sales_amount_excl_tax), with a zero-denominator guard. Use DISTINCTCOUNT(invoice_id) for invoice count. Never sum unit prices or percentages.
# 
# The inspected batch has no credit notes. This first model **stops if any credit notes appear** so their sign/tax policy can be designed explicitly. It does not silently exclude them or negate amounts. Positive quantities, nonnegative prices/tax, and non-null required measures are checked for this regular-invoice model.
# 
# Customer geography uses `delivery_city_id`. Product prices on the fact come from invoice lines, never from current catalogue prices. Optional product colour/brand/size stay NULL when absent. A NULL colour key is allowed; a non-null unmatched colour key is rejected. No personally identifying contact fields, photos, or spatial binaries are copied to Gold.
# 
# ## Reliability and expected results
# Inputs are read at fixed Delta versions. Each input must match the selected Silver RunID, Bronze BatchID and logged row count; a partial later Silver overwrite causes a stop, not automatic historical recovery. Recorded input versions support reproducibility while Delta retention allows it.
# 
# Expected initial rows: date 1,461 (2013–2016), customer 663, stock item 227, fact 228,265. These values are informational only; executable checks derive expectations from the selected data.
# 
# Operational tables: `common.gold_run_log`, `common.gold_input_versions`, `common.gold_validation_results`, `common.gold_table_results`, `common.gold_failures`. Failed checks are logged and raise exceptions. If storage itself is unavailable, logging can fail; the notebook still raises the original processing error.
# 
# Allow only one Gold sales writer at a time; do not launch a concurrent manual/pipeline run. Delta commits are per-table, not across the model. A failure after writing starts can leave mixed Gold tables. Downstream reporting refresh must be gated on COMPLETED and the four tables' `_gold_run_id`. Each retry has a fresh Gold RunID and rebuilds all four tables. Do not manually rerun only the final cells after a failure.
# 
# The four business outputs use managed Delta overwrite without blanket schema replacement. An incompatible existing schema stops for explicit migration. All outputs carry `_gold_run_id`, `_silver_run_id`, `_bronze_batch_id`, `_processed_at_utc`.
# 
# References: [Delta versioned reads](https://docs.delta.io/delta-batch/), [Fabric time travel](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-lake-time-travel), [Fabric notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity).


# MARKDOWN ********************

# **Cell 1 — Parameters**
# 
# Expected: no output. Ensure this is marked as the parameter cell after import.

# PARAMETERS CELL ********************

p_silver_run_id = ""
p_pipeline_exit = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 2 — Initialize**
# 
# Expected: a fresh Gold RunID. All dates and technical timestamps use UTC.

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 3 — Gold operational tables**
# 
# Creates five missing log tables under the existing common schema. Preserves previous logs; does not initialize Bronze or Silver.

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

# MARKDOWN ********************

# **Cell 4 — Logging and quality helpers**
# 
# Definitions only. Every recorded check has a status; no failing record is silently dropped.

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
           "load_type": "FULL", "run_status": status, "expected_table_count": 4,
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

# MARKDOWN ********************

# **Cell 5 — Input contracts and completed Silver run**
# 
# Definitions only. Uses actual schema names supplied in this project. Rejects missing fields or type changes before transformations.

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
SOURCE_KEYS = {
 "sales.invoices":"invoice_id", "sales.invoice_lines":"invoice_line_id",
 "sales.customers":"customer_id", "sales.customer_categories":"customer_category_id",
 "reference.cities":"city_id", "reference.state_provinces":"state_province_id",
 "reference.countries":"country_id", "inventory.stock_items":"stock_item_id",
 "inventory.colors":"color_id", "inventory.package_types":"package_type_id"
}

def select_silver_run():
    runs = spark.table(f"{SILVER}.quality.silver_run_log")
    if STATE["silver_run_id"]:
        rows = runs.filter(F.col("silver_run_id") == STATE["silver_run_id"]).collect()
        check("__RUN__", "EXPLICIT_SILVER_RUN_MATCHES", len(rows), 1)
    else:
        rows = (runs.filter(F.col("run_status") == "COMPLETED")
                .orderBy(F.col("completed_at_utc").desc_nulls_last()).limit(2).collect())
        if not rows: raise ValueError("No completed Silver run exists")
        if len(rows) > 1 and rows[0]["completed_at_utc"] == rows[1]["completed_at_utc"]:
            raise ValueError("Tied Silver completion times: supply p_silver_run_id")
    r = rows[0]
    STATE["silver_run_id"] = r["silver_run_id"]
    STATE["bronze_batch_id"] = r["batch_id"]
    check("__RUN__", "SILVER_RUN_COMPLETED", r["run_status"], "COMPLETED")
    check("__RUN__", "SILVER_FULL_RUN", r["load_type"], "FULL")
    check("__RUN__", "SILVER_ALL_TABLES", r["completed_table_count"], r["expected_table_count"])
    check("__RUN__", "SILVER_EXPECTED_TABLES", r["expected_table_count"], 48)
    check("__RUN__", "SILVER_FAILURES", r["failed_table_count"], 0)
    if not r["batch_id"] or r["completed_at_utc"] is None: raise ValueError("Incomplete Silver run identity")
    results = (spark.table(f"{SILVER}.quality.silver_table_results")
               .filter(F.col("silver_run_id") == STATE["silver_run_id"])
               .withColumn("qualified_table", F.concat_ws(".","silver_schema","silver_table"))
               .filter(F.col("qualified_table").isin(list(CONTRACTS))).collect())
    check("__RUN__", "INPUT_RESULT_COUNT", len(results), len(CONTRACTS))
    check("__RUN__", "INPUT_RESULT_SET", sorted(r["qualified_table"] for r in results), sorted(CONTRACTS))
    counts = {}
    for r in results:
        t = r["qualified_table"]
        check(t, "SILVER_TABLE_VALIDATED", r["table_status"], "VALIDATED")
        check(t, "SILVER_TABLE_BATCH", r["batch_id"], STATE["bronze_batch_id"])
        check(t, "SILVER_LOG_COUNT_MATCH", r["actual_row_count"], r["expected_row_count"])
        if r["actual_row_count"] is None or r["actual_row_count"] <= 0:
            raise ValueError(f"Required sales-model input is empty: {t}")
        counts[t] = int(r["actual_row_count"])
    return counts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 6 — Pin and validate physical Silver inputs**
# 
# Definitions only. Reads exact Delta versions and verifies run identity/counts. A different or missing RunID fails, even if an earlier run log says COMPLETED.

# CELL ********************

def read_inputs(counts):
    inputs = {}
    for table, contract in CONTRACTS.items():
        STATE["table"] = table
        full_name = f"{SILVER}.{table}"
        delta = DeltaTable.forName(spark, full_name)
        detail = delta.detail().select("id","location").first()
        version = int(delta.history(1).select("version").first()["version"])
        df = (spark.read.format("delta").option("versionAsOf", version).load(detail["location"]))
        types = {f.name:f.dataType.simpleString() for f in df.schema.fields}
        required = {**contract, "_silver_run_id":"string", "_batch_id":"string"}
        differences = [f"{c}: expected {dtype}, got {types.get(c)}" for c,dtype in required.items() if types.get(c)!=dtype]
        check(table, "SCHEMA_CONTRACT", differences, [])
        # Read only fields used by this model, with lineage for verification.
        df = hold(df.select(*required.keys()))
        bad = (~F.col("_silver_run_id").eqNullSafe(F.lit(STATE["silver_run_id"]))
               | ~F.col("_batch_id").eqNullSafe(F.lit(STATE["bronze_batch_id"])))
        metrics = df.agg(F.count("*").alias("rows"),
                        F.coalesce(F.sum(F.when(bad,1).otherwise(0)),F.lit(0)).alias("wrong_run")).first()
        check(table, "PHYSICAL_ROW_COUNT", int(metrics["rows"]), counts[table])
        check(table, "PHYSICAL_RUN_IDENTITY", int(metrics["wrong_run"]), 0)
        unique_key(df, [SOURCE_KEYS[table]], table)
        log_append("gold_input_versions", [{"gold_run_id":GOLD_RUN_ID,
          "silver_run_id":STATE["silver_run_id"], "bronze_batch_id":STATE["bronze_batch_id"],
          "source_table":full_name, "delta_table_id":detail["id"], "delta_version":version,
          "delta_location":detail["location"], "expected_row_count":counts[table],
          "actual_row_count":int(metrics["rows"]), "checked_at_utc":utc_now()}])
        inputs[table] = df
        print(f"INPUT PASSED: {table} ({metrics['rows']:,} rows, version {version})")
    return inputs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 7 — Build the date dimension**
# 
# Definition only. Generates full calendar years and checks every invoice date has a match.

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

# MARKDOWN ********************

# **Cell 8 — Build current customer and product dimensions**
# 
# Definitions only. All joins are checked against unique parent keys. Optional NULL colour remains NULL; non-null orphan keys stop processing.

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

# MARKDOWN ********************

# **Cell 9 — Build invoice-line fact and reconcile amounts**
# 
# Definition only. Uses invoice-line prices, preserves one row per invoice line and checks exact Decimal totals. Future credit notes stop for a separate policy.

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

# MARKDOWN ********************

# **Cell 10 — Validate and publish managed Gold tables**
# 
# Definitions only. All four in-memory outputs pass before the first business write. Post-write schema, lineage, counts and exact content are checked.

# CELL ********************

OUTPUT_KEYS = {"common.dim_date":"date_key", "sales.dim_customer":"customer_key",
               "inventory.dim_stock_item":"stock_item_key", "sales.fact_invoice_line":"invoice_line_id"}

def validate_output(table,df,expected):
    check(table,"OUTPUT_ROW_COUNT",df.count(),expected)
    unique_key(df,[OUTPUT_KEYS[table]],table)
    if table=="common.dim_date": unique_key(df,["calendar_date"],table)
    identities={"_gold_run_id":GOLD_RUN_ID,"_silver_run_id":STATE["silver_run_id"],"_bronze_batch_id":STATE["bronze_batch_id"]}
    bad=reduce(lambda a,b:a|b,(~F.col(k).eqNullSafe(F.lit(v)) for k,v in identities.items()))
    check(table,"OUTPUT_LINEAGE",invalid_count(df,bad|F.col("_processed_at_utc").isNull()),0)

def publish(table,df,expected):
    STATE["table"]=table; STATE["stage"]="GOLD_WRITE"
    target=f"{GOLD}.{table}"
    df.write.format("delta").mode("overwrite").saveAsTable(target)
    STATE["stage"]="POST_WRITE_VALIDATION"
    written=spark.table(target)
    check(table,"WRITTEN_SCHEMA",[(f.name,f.dataType.simpleString()) for f in written.schema.fields],
          [(f.name,f.dataType.simpleString()) for f in df.schema.fields])
    validate_output(table,written,expected)
    # exceptAll preserves duplicates; comparing both directions verifies exact contents.
    check(table,"MISSING_WRITTEN_CONTENT",df.exceptAll(written.select(df.columns)).limit(1).count(),0)
    check(table,"EXTRA_WRITTEN_CONTENT",written.select(df.columns).exceptAll(df).limit(1).count(),0)
    log_append("gold_table_results",[{"gold_run_id":GOLD_RUN_ID,"table_name":table,"table_status":"VALIDATED",
                "expected_row_count":expected,"actual_row_count":expected,"completed_at_utc":utc_now()}])
    STATE["completed_tables"]+=1
    flush_checks(); save_run("RUNNING")
    print(f"GOLD VERIFIED: {target} ({expected:,} rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 11 — Execute the complete Gold build**
# 
# Expected: four GOLD VERIFIED messages and COMPLETED. If anything fails, inspect common.gold_failures and common.gold_validation_results for this Gold RunID, then rerun all cells after fixing it.

# CELL ********************

try:
    save_run("RUNNING")
    STATE["stage"]="SILVER_RUN_SELECTION"
    counts=select_silver_run()
    save_run("RUNNING")
    STATE["stage"]="SILVER_INPUT_VERIFICATION"
    inputs=read_inputs(counts)
    flush_checks()

    STATE["stage"]="BUILD_DATE"; STATE["table"]="common.dim_date"
    dates,date_count=build_date(inputs)
    STATE["stage"]="BUILD_CUSTOMER"; STATE["table"]="sales.dim_customer"
    customers=build_customer(inputs)
    STATE["stage"]="BUILD_PRODUCT"; STATE["table"]="inventory.dim_stock_item"
    products=build_product(inputs)
    STATE["stage"]="BUILD_FACT"; STATE["table"]="sales.fact_invoice_line"
    fact,baseline=build_fact(inputs,dates,customers,products)
    outputs={"common.dim_date":hold(add_lineage(dates)),"sales.dim_customer":hold(add_lineage(customers)),
             "inventory.dim_stock_item":hold(add_lineage(products)),"sales.fact_invoice_line":hold(add_lineage(fact))}
    expected={"common.dim_date":date_count,"sales.dim_customer":counts["sales.customers"],
              "inventory.dim_stock_item":counts["inventory.stock_items"],"sales.fact_invoice_line":counts["sales.invoice_lines"]}
    STATE["stage"]="PRE_WRITE_VALIDATION"
    for table,df in outputs.items():
        STATE["table"]=table
        validate_output(table,df,expected[table])
    flush_checks()
    print("All four Gold outputs validated before writing.")
    for table,df in outputs.items(): publish(table,df,expected[table])

    STATE["stage"]="FINAL_RECONCILIATION"; STATE["table"]="__RUN__"
    check("__RUN__","COMPLETED_TABLES",STATE["completed_tables"],4)
    current={table:spark.table(f"{GOLD}.{table}") for table in outputs}
    for table,df in current.items(): validate_output(table,df,expected[table])
    f=current["sales.fact_invoice_line"]
    foreign_key(f,"date_key",current["common.dim_date"],"date_key","FINAL/date")
    foreign_key(f,"customer_key",current["sales.dim_customer"],"customer_key","FINAL/customer")
    foreign_key(f,"bill_to_customer_key",current["sales.dim_customer"],"customer_key","FINAL/bill_to_customer")
    foreign_key(f,"stock_item_key",current["inventory.dim_stock_item"],"stock_item_key","FINAL/product")
    reconcile_fact(f,baseline,"FINAL/fact")
    flush_checks()
    STATE["fact_rows"]=expected["sales.fact_invoice_line"]
    save_run("COMPLETED")
    RUN_COMPLETED=True
    GOLD_RESULT={"status":"COMPLETED","gold_run_id":GOLD_RUN_ID,"silver_run_id":STATE["silver_run_id"],
                 "bronze_batch_id":STATE["bronze_batch_id"],"tables":4,"row_counts":expected,
                 "sales_totals":{k:str(v) for k,v in baseline.items()}}
    print(json.dumps(GOLD_RESULT,indent=2))
except Exception as error:
    # Attempt each log independently; preserve the original failure even when logging fails.
    failure={"gold_run_id":GOLD_RUN_ID,"silver_run_id":STATE["silver_run_id"],"processing_stage":STATE["stage"],
             "table_name":STATE["table"],"error_type":type(error).__name__,"error_message":str(error)[:8000],"failed_at_utc":utc_now()}
    for action in [flush_checks,lambda:log_append("gold_failures",[failure]),lambda:save_run("FAILED",error)]:
        try: action()
        except Exception as logging_error: print("Quality logging also failed:",str(logging_error)[:500])
    raise
finally:
    for frame in HELD:
        try: frame.unpersist()
        except Exception as cleanup_error: print("Cache cleanup:",str(cleanup_error)[:200])
    HELD.clear()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Cell 12 — Final result / pipeline exit**
# 
# Returns a completion object for orchestration. Keep outside try/except because notebook exit is control flow. Next stage is pipeline wiring and Power BI relationships.

# CELL ********************

if not RUN_COMPLETED or GOLD_RESULT is None:
    raise RuntimeError("Gold build did not complete. Run the notebook in order after resolving the failure.")
if str(p_pipeline_exit).lower()=="true":
    notebookutils.notebook.exit(json.dumps(GOLD_RESULT))
else:
    print("Gold sales model completed. Review common.gold_run_log for Gold RunID:",GOLD_RUN_ID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Power BI relationship plan (after successful Gold pipeline run)
# 
# - `common.dim_date.date_key` (1) → `sales.fact_invoice_line.date_key` (*)
# - `sales.dim_customer.customer_key` (1) → `sales.fact_invoice_line.customer_key` (*)
# - `inventory.dim_stock_item.stock_item_key` (1) → `sales.fact_invoice_line.stock_item_key` (*)
# - Optional billing-customer analysis: use a separate role-playing customer dimension or an inactive relationship to `bill_to_customer_key`; do not create two active competing paths.
# 
# Use single-direction dimension-to-fact filtering. Sort `month_name` by `month_number`, `day_name` by `day_of_week_number`, and `year_month` by `year_month_key`. Mark the date dimension using `calendar_date`. Additional revenue aggregates are optional; the detailed fact already supports the agreed KPIs.
# 
# **Validation boundary:** local syntax and synthetic Spark tests can verify transformation logic, but do not prove access to Fabric, Delta writes, actual source joins or capacity. This notebook executes those gates against your real tables and stops on failure. Do not mark Gold complete merely because importing the file succeeds.
# 
# 
# Local verification completed with PySpark 3.5.3: all 12 cells compile; notebook schema valid; source contracts match the supplied schema export. Synthetic tests passed for date range/leap day, current-dimension joins, optional NULL colour, billing-customer role key, exact Decimal totals, output lineage, and rejection of duplicate parent keys, orphan invoice lines, credit notes and amount mismatches. Fabric access and Delta publication were not executed locally.

