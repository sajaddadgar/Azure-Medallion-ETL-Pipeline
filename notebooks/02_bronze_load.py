# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Read the raw files

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
BRONZE = "workspace.tlc_bronze"
SERVICE = "yellow"
VOLUME_ROOT = "/Volumes/workspace/tlc_bronze/landing"



raw = (
    spark.read
    .option("basePath", f"{VOLUME_ROOT}/raw/tlc/{SERVICE}/")
    .parquet(f"{VOLUME_ROOT}/raw/tlc/{SERVICE}/")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

print(f"Rows read: {raw.count():,}")
raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Write the Delta table with safe rerun

# COMMAND ----------

table = f"{BRONZE}.trips_raw"

if spark.catalog.tableExists(table):
    spark.sql(f"DELETE FROM {table} WHERE _service = '{SERVICE}'")
    raw.withColumn("_service", F.lit(SERVICE)).write.mode("append").saveAsTable(table)
else:
    raw.withColumn("_service", F.lit(SERVICE)).write.mode("append").partitionBy("month").saveAsTable(table)
    spark.sql(f"COMMENT ON TABLE {table} IS 'Bronze: NYC TLC trip records exactly as landed by ADF. Lineage cols prefixed _.'")

print(f"{table}: {spark.table(table).count():,} rows")
display(spark.sql(f"SELECT month, count(*) AS trips FROM {table} GROUP BY month ORDER BY month"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. The zone dimension

# COMMAND ----------

zone_table = f"{BRONZE}.zone_lookup_raw"
zones_raw = (
    spark.read.option("header", True).option("inferSchema", False)
    .csv(f"{VOLUME_ROOT}/raw/tlc/reference/taxi_zone_lookup.csv")
    .withColumn("_ingested_at", F.current_timestamp())
)

zones_raw.write.mode('overwrite').saveAsTable(zone_table)

print(f"{BRONZE}.zone_lookup_raw: {spark.table(f'{BRONZE}.zone_lookup_raw').count()} zones")
display(zones_raw.limit(5))