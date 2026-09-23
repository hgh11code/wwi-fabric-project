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

# CELL ********************

# Configure the three snapshot exceptions and initialize missing watermarks.
# The original full load is our baseline before simulation began on June 1.
# Existing watermarks are preserved; this does not copy any business data.

from delta.tables import DeltaTable
from pyspark.sql import functions as F

BASELINE_BATCH = "20f2f6b1-aea1-4093-a63f-f649bc5447c8"
BASELINE_END = "2016-05-31T23:59:59.9999999"

config = spark.table("ops.table_config")

# Fail before making changes if configuration is incomplete or duplicated.
keys = ["SourceSystem", "SourceDatabase", "SourceSchema", "SourceTable"]

if config.count() != 48 or config.select(*keys).distinct().count() != 48:
    raise RuntimeError("Expected exactly 48 unique configured tables.")

snapshot_condition = """
    (SourceSchema = 'Application'
     AND SourceTable IN ('Countries', 'Countries_Archive'))
    OR
    (SourceSchema = 'Warehouse'
     AND SourceTable = 'ColdRoomTemperatures')
"""

DeltaTable.forName(spark, "ops.table_config").update(
    condition=snapshot_condition,
    set={
        "IncrementalMethod": "'SNAPSHOT'",
        "UpdatedAtUTC": "current_timestamp()"
    }
)

# Snapshot tables do not need a timestamp watermark.
# For other tables, insert a starting watermark only when none exists.
candidates = (
    spark.table("ops.table_config")
    .filter(F.col("IncrementalMethod") != "SNAPSHOT")
    .select(*keys, "WatermarkColumn")
    .withColumn("PreviousWatermark", F.lit(None).cast("string"))
    .withColumn("CurrentWatermark", F.lit(BASELINE_END))
    .withColumn("LastSuccessfulBatchID", F.lit(BASELINE_BATCH))
    .withColumn("WatermarkStatus", F.lit("INITIALIZED"))
    .withColumn("UpdatedAtUTC", F.current_timestamp())
)

if candidates.filter(
    F.col("WatermarkColumn").isNull()
    | (F.trim(F.col("WatermarkColumn")) == "")
).count():
    raise RuntimeError("An incremental table has no watermark column.")

match_condition = " AND ".join(
    f"target.{key} = source.{key}" for key in keys
)

(
    DeltaTable.forName(spark, "ops.watermarks")
    .alias("target")
    .merge(candidates.alias("source"), match_condition)
    .whenNotMatchedInsertAll()
    .execute()
)

print("Setup saved. Existing watermarks were preserved.")

display(
    spark.table("ops.table_config")
    .groupBy("IncrementalMethod")
    .count()
    .orderBy("IncrementalMethod")
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
