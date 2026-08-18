SELECT pickup_borough,
       sum(trips)                        AS trips,
       round(sum(revenue)/1e6, 1)        AS revenue_millions,
       round(avg(avg_fare), 2)           AS avg_fare,
       round(avg(avg_tip_pct), 1)        AS avg_tip_pct
FROM workspace.tlc_gold.daily_borough_revenue
GROUP BY pickup_borough
ORDER BY trips DESC;