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

# Define the last completed business day's extraction boundary.
# The pipeline overrides this value through its existing upper_watermark parameter.
# Include the complete day because simulator activity continues after noon.

upper_watermark = "2016-06-03T23:59:59.9999999"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build extraction and count queries from configuration and saved watermarks.
# Snapshot tables extract their complete current contents.
# Fail on missing or invalid settings instead of silently skipping tables.

import json
import re
from datetime import datetime
from pyspark.sql import functions as F

def normalize_watermark(value):
    text = str(value).strip().replace(" ", "T")
    if text.endswith("Z"):
        text = text[:-1]
    if not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,7})?",
        text
    ):
        raise ValueError(f"Invalid watermark: {value!r}")
    datetime.strptime(text[:19], "%Y-%m-%dT%H:%M:%S")
    main, _, fraction = text.partition(".")
    return main + "." + fraction.ljust(7, "0")

def identifier(value):
    return "[" + value.replace("]", "]]") + "]"

def sql_text(value):
    return "N'" + value.replace("'", "''") + "'"

upper = normalize_watermark(upper_watermark)

if not upper.endswith("T23:59:59.9999999"):
    raise ValueError("Upper watermark must cover the complete business day.")

keys = ["SourceSystem", "SourceDatabase", "SourceSchema", "SourceTable"]

configuration = (
    spark.table("ops.table_config")
    .filter(F.col("IsActive") & F.col("IncrementalEnabled"))
)

watermarks = spark.table("ops.watermarks")

for label, frame in [
    ("configuration", configuration),
    ("watermarks", watermarks)
]:
    if frame.groupBy(*keys).count().filter("count > 1").count():
        raise RuntimeError(f"Duplicate table keys in {label}.")

rows = (
    configuration.join(
        watermarks.select(*keys, "CurrentWatermark"),
        keys,
        "left"
    )
    .orderBy("SourceSchema", "SourceTable")
    .collect()
)

if len(rows) != 48:
    raise RuntimeError(f"Expected 48 enabled tables; found {len(rows)}.")

supported = {
    "SNAPSHOT",
    "TEMPORAL_CURRENT_UPSERT",
    "TEMPORAL_HISTORY_APPEND",
    "WATERMARK_UPSERT",
    "WATERMARK_APPEND"
}

table_list = []

for row in rows:
    schema = row["SourceSchema"]
    table = row["SourceTable"]
    method = row["IncrementalMethod"]
    column = row["WatermarkColumn"]
    qualified = f"{identifier(schema)}.{identifier(table)}"

    if method not in supported:
        raise RuntimeError(f"Unsupported method for {qualified}: {method}")

    lower = None
    where = ""

    if method != "SNAPSHOT":
        if not row["CurrentWatermark"] or not column:
            raise RuntimeError(f"Missing watermark settings: {qualified}")

        lower = normalize_watermark(row["CurrentWatermark"])

        if lower > upper:
            raise RuntimeError(f"Watermark is ahead of this run: {qualified}")

        where = (
            f" WHERE {identifier(column)}"
            f" > CONVERT(datetime2(7), {sql_text(lower)}, 126)"
            f" AND {identifier(column)}"
            f" <= CONVERT(datetime2(7), {sql_text(upper)}, 126)"
        )

    # Require generation to be complete through exactly the requested day.
    # This also prevents calling a present-day snapshot a historical snapshot.
    guard = f"""
SET NOCOUNT ON;
IF NOT EXISTS (
    SELECT 1 FROM ops.simulation_days
    WHERE BusinessDate = CONVERT(date, {sql_text(upper[:10])}, 23)
)
    THROW 51000, 'Requested simulation day is not completed.', 1;
IF EXISTS (
    SELECT 1 FROM ops.simulation_days
    WHERE BusinessDate > CONVERT(date, {sql_text(upper[:10])}, 23)
)
    THROW 51001, 'Newer simulated days exist. Update the upper watermark.', 1;
"""

    # Azure SQL builds the column list from its own table metadata.
    # Geography keeps the original binary serialization; datetime keeps precision.
    source_query = guard + f"""
DECLARE @columns nvarchar(max);
DECLARE @query nvarchar(max);

SELECT @columns =
    STRING_AGG(
        CAST(
            CASE
                WHEN t.name IN ('datetime2', 'datetime', 'smalldatetime')
                    THEN N'CONVERT(varchar(33), '
                         + QUOTENAME(c.name)
                         + N', 126) AS ' + QUOTENAME(c.name)
                WHEN t.name IN ('geography', 'geometry')
                    THEN QUOTENAME(c.name)
                         + N'.Serialize() AS ' + QUOTENAME(c.name)
                ELSE QUOTENAME(c.name)
            END AS nvarchar(max)
        ),
        N', '
    ) WITHIN GROUP (ORDER BY c.column_id)
FROM sys.columns AS c
JOIN sys.types AS t ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID({sql_text(qualified)});

IF @columns IS NULL
    THROW 51002, 'Source table columns could not be read.', 1;

SET @query = N'SELECT ' + @columns
           + {sql_text(" FROM " + qualified + where + ";")};

EXEC sys.sp_executesql @query;
"""

    table_list.append({
        "SourceSystem": row["SourceSystem"],
        "SourceDatabase": row["SourceDatabase"],
        "SourceSchema": schema,
        "SourceTable": table,
        "TableType": row["TableType"],
        "ExtractionMethod": method,
        "LoadType": "snapshot" if method == "SNAPSHOT" else "incremental",
        "WatermarkColumn": column,
        "LowerWatermark": lower,
        "UpperWatermark": upper,
        "AdvanceWatermark": method != "SNAPSHOT",
        "SourceQuery": source_query,
        "SourceCountQuery": (
            guard
            + f"SELECT COUNT_BIG(*) AS SourceRowCount "
              f"FROM {qualified}{where};"
        )
    })

print(f"Prepared {len(table_list)} tables.")
print(f"Upper watermark: {upper}")
print("No data copied and no watermarks changed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Return the extraction plan to the master pipeline.
# Its ForEach activity uses TableList to process the configured tables.
# Keep this as the final cell.

notebookutils.notebook.exit(
    json.dumps({"TableList": table_list})
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
