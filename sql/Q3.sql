SELECT day_of_week,
       sum(trips)                  AS trips,
       round(avg(avg_fare), 2)     AS avg_fare,
       round(avg(avg_minutes), 1)  AS avg_minutes
FROM workspace.tlc_gold.hourly_demand
GROUP BY day_of_week
ORDER BY trips DESC;