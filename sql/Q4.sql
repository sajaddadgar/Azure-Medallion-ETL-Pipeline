SELECT pickup_zone, dropoff_zone, trips,
       avg_fare, avg_miles, avg_minutes, avg_tip_pct
FROM workspace.tlc_gold.zone_pair_flows
ORDER BY trips DESC
LIMIT 15;