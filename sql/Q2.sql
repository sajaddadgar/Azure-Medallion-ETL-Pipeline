SELECT pickup_hour,
       sum(trips)            AS trips,
       round(avg(avg_mph),1) AS avg_mph,
       round(avg(avg_fare),2) AS avg_fare
FROM workspace.tlc_gold.hourly_demand
GROUP BY pickup_hour
ORDER BY pickup_hour;