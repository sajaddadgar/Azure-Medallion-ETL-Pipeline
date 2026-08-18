# Databricks notebook source
from pyspark.sql import functions as F, Window as W


BRONZE = "workspace.tlc_bronze"
SILVER = "workspace.tlc_silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Types + derived columns

# COMMAND ----------

typed = (
    spark.table(f"{BRONZE}.trips_raw").select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("PULocationID").cast("int").alias("pickup_zone_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_zone_id"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("month"),
)
# derived business columns — computed once here so nobody recomputes them downstream
    .withColumn("trip_date",     F.to_date("pickup_ts"))
    .withColumn("pickup_hour",   F.hour("pickup_ts"))
    .withColumn("day_of_week",   F.date_format("pickup_ts", "EEEE"))
    .withColumn("duration_min",  (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0)
    .withColumn("tip_pct",       F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount")))
    .withColumn("mph",           F.when(F.col("duration_min") > 0, F.col("trip_distance") / (F.col("duration_min") / 60.0)))
)

print("Typed columns:", len(typed.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Data quality gates → quarantine, not silent drops

# COMMAND ----------

checked = typed.withColumn(
    "_dq_failures",
    F.array_compact(F.array(
        F.when(F.col("pickup_ts").isNull() |  F.col("dropoff_ts").isNull(), F.lit("null_timestamp")),
        F.when(F.col("dropoff_ts") <= F.col("pickup_ts"), F.lit("dropoff_not_after_pickup")),
        F.when(F.col("duration_min") > 720, F.lit("duration_over_12h")),
        F.when(F.col("trip_distance") <= 0, F.lit("non_positive_distance")),
        F.when(F.col("trip_distance") > 200, F.lit("distance_over_200mi")),
        F.when(F.col("total_amount") < 0, F.lit("negative_total")),
        F.when(F.col("total_amount") > 5000, F.lit("total_over_5000")),
        F.when(F.col("mph") > 100, F.lit("implausible_speed")),
        F.when((F.col("passenger_count").isNull()) | (F.col("passenger_count") < 0), F.lit("bad_passenger_count")),
        F.when(F.year("pickup_ts") != F.lit(2025), F.lit("date_outside_2025")),
    )),
)


valid = checked.where(F.size("_dq_failures") == 0).drop("_dq_failures")
quarantine = (checked.where(F.size("_dq_failures") > 0)
              .withColumn("_dq_rule", F.concat_ws(",", "_dq_failures"))
              .drop("_dq_failures")
              .withColumn("_quarantined_at", F.current_timestamp())
)

quarantine.write.mode("overwrite").saveAsTable(f"{SILVER}.trips_quarantine")


q = spark.table(f"{SILVER}.trips_quarantine").count()
print(f"Quarantined: {q:,}")
display(spark.sql(f"SELECT _dq_rule, count(*) AS n FROM {SILVER}.trips_quarantine GROUP BY _dq_rule ORDER BY n DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Dedupe + MERGE upsert

# COMMAND ----------

# The duplicates
key_w = W.partitionBy("vendor_id", "pickup_ts", "dropoff_ts", "pickup_zone_id", "dropoff_zone_id", "total_amount")

deduped = (valid.withColumn("_rn", F.row_number().over(key_w.orderBy(F.col("pickup_ts"))))
           .where(F.col("_rn") == 1)
           .withColumn("trip_id", F.sha2(F.concat_ws("|", "vendor_id", "pickup_ts", "dropoff_ts", "pickup_zone_id", "dropoff_zone_id", "total_amount"), 256))
           .withColumn("_updated_at", F.current_timestamp()))

fact = f"{SILVER}.trips"
deduped.createOrReplaceTempView("silver_batch_v")


if not spark.catalog.tableExists(fact):
    deduped.write.mode("overwrite").partitionBy("month").saveAsTable(fact)
    spark.sql(f"COMMENT ON table {fact} IS 'Silver: cleaned, typed, deduplicated trips with derived features. Keyed by trip_id.'")
    print("Created", fact)
else:
    spark.sql(f"""MERGE INTO {fact} AS t
               USING silver_batch_v as s ON t.trip_id = s.trip_id
               WHEN MATCHED THEN UPDATE SET *
               WHEN NOT MATCHED THEN INSERT *""")
    
print(f"{fact}: {spark.table(fact).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. The dimension

# COMMAND ----------

dim = (
    spark.table(f"{BRONZE}.zone_lookup_raw").select(
        F.col("LocationID").cast("int").alias("zone_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone_name"),
        F.col("service_zone").alias("service_zone"),
    ).where(F.col("zone_id").isNotNull()).dropDuplicates(["zone_id"])
)


dim.write.mode("overwrite").saveAsTable(f"{SILVER}.dim_zone")
print(f"{SILVER}.dim_zone: {spark.table(f'{SILVER}.dim_zone').count()} zones")

# COMMAND ----------

display(spark.sql(f"""
    SELECT t.trip_date, t.pickup_hour, z.borough, z.zone_name,
           ROUND(t.trip_distance,2) AS miles, ROUND(t.duration_min,1) AS minutes,
           ROUND(t.mph,1) AS mph, t.total_amount, ROUND(t.tip_pct*100,1) AS tip_pct
    FROM {SILVER}.trips t
    JOIN {SILVER}.dim_zone z ON t.pickup_zone_id = z.zone_id
    LIMIT 20
"""))