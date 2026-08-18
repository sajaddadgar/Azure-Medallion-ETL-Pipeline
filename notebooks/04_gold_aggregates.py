# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Daily borough revenue

# COMMAND ----------

SILVER = "workspace.tlc_silver"
GOLD = "workspace.tlc_gold"

# COMMAND ----------

spark.sql(f"""
          CREATE OR REPLACE TABLE {GOLD}.daily_borough_revenue
          COMMENT 'Gold: trips, revenue, tips and averages per pickup borough per day.'
          AS
          SELECT 
            z.borough AS pickup_borough,
            t.trip_date,
            t.month,
            count(*) AS trips,
            ROUND(SUM(t.total_amount), 2) AS revenue,
            ROUND(SUM(t.tip_amount), 2) AS tips,
            ROUND(AVG(t.total_amount), 2) AS avg_fare,
            ROUND(AVG(t.trip_distance), 2) AS avg_miles,
            ROUND(AVG(t.duration_min), 1) AS avg_minutes,
            ROUND(AVG(t.tip_pct) * 100, 1) AS avg_tip_pct,
            ROUND(SUM(t.total_amount) / sum(t.trip_distance), 2) AS revenue_per_mile
          from {SILVER}.trips t
          JOIN {SILVER}.dim_zone z ON t.pickup_zone_id = z.zone_id
          GROUP BY z.borough, t.trip_date, t.month
          """)


print("daily_borough_revenue:", spark.table(f"{GOLD}.daily_borough_revenue").count(), "rows")
display(spark.sql(f"SELECT * FROM {GOLD}.daily_borough_revenue"))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 2. Hourly demand profile:

# COMMAND ----------

spark.sql(f"""
          CREATE OR REPLACE TABLE {GOLD}.hourly_demand
          COMMENT 'Gold: demand curve by day-of-week and hour — the classic staffing/surge question.'
          AS
          SELECT 
            t.day_of_week,
            t.pickup_hour,
            COUNT(*) AS trips,
            ROUND(AVG(t.total_amount), 2) AS avg_fare,
            ROUND(AVG(t.duration_min), 2) AS avg_minutes,
            ROUND(AVG(t.mph), 2) AS avg_mph,
            ROUND(COUNT(*) / COUNT(DISTINCT t.trip_date), 0) AS avg_trips_per_day
           FROM {SILVER}.trips t
           GROUP BY t.day_of_week, t.pickup_hour
          """)

print("hourly_demand:", spark.table(f"{GOLD}.hourly_demand").count(), "rows")   # 7 × 24 = 168
display(spark.sql(f"SELECT * FROM {GOLD}.hourly_demand"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Zone-pair flows

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.zone_pair_flows
COMMENT 'Gold: top origin→destination corridors (>=500 trips) with economics.'
AS
SELECT
    pz.borough AS pickup_borough,  pz.zone_name AS pickup_zone,
    dz.borough AS dropoff_borough, dz.zone_name AS dropoff_zone,
    count(*)                       AS trips,
    round(sum(t.total_amount), 2)  AS revenue,
    round(avg(t.total_amount), 2)  AS avg_fare,
    round(avg(t.trip_distance), 2) AS avg_miles,
    round(avg(t.duration_min), 1)  AS avg_minutes,
    round(avg(t.tip_pct) * 100, 1) AS avg_tip_pct
FROM {SILVER}.trips t
JOIN {SILVER}.dim_zone pz ON t.pickup_zone_id  = pz.zone_id
JOIN {SILVER}.dim_zone dz ON t.dropoff_zone_id = dz.zone_id
GROUP BY pz.borough, pz.zone_name, dz.borough, dz.zone_name
HAVING count(*) >= 500
""")
print("zone_pair_flows:", spark.table(f"{GOLD}.zone_pair_flows").count(), "rows")
display(spark.sql(f"SELECT * FROM {GOLD}.zone_pair_flows"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Peek at the payoff

# COMMAND ----------

display(spark.sql(f"""
    SELECT pickup_borough, sum(trips) AS trips, round(sum(revenue)/1e6, 1) AS revenue_millions,
           round(avg(avg_tip_pct), 1) AS avg_tip_pct
    FROM {GOLD}.daily_borough_revenue
    GROUP BY pickup_borough ORDER BY trips DESC
"""))

display(spark.sql(f"""
    SELECT pickup_hour, sum(trips) AS trips, round(avg(avg_mph), 1) AS avg_mph
    FROM {GOLD}.hourly_demand GROUP BY pickup_hour ORDER BY pickup_hour
"""))