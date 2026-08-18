SELECT month,
       sum(trips)                 AS trips,
       round(sum(revenue)/1e6, 1) AS revenue_millions
FROM workspace.tlc_gold.daily_borough_revenue
GROUP BY month
ORDER BY month;