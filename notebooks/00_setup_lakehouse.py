# Databricks notebook source
# Where everything lives in Free Edition
CATALOG = "workspace"

# One schema per medallion layer — IF NOT EXISTS makes reruns harmless
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.tlc_bronze COMMENT 'Bronze: raw NYC TLC trip data, as ingested'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.tlc_silver COMMENT 'Silver: cleansed, typed, deduplicated trips'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.tlc_gold COMMENT 'Gold: business-level aggregates'")


# A Volume = governed file storage; Step 8 lands the ADLS raw files here
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.tlc_bronze.landing COMMENT 'Landing area for files pulled from the ADLS raw zone'")


print("Lakehouse structure created.")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE 'tlc_*'"))
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.tlc_bronze"))