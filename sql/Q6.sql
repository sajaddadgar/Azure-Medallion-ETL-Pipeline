SELECT _dq_rule,
       count(*) AS rows_quarantined,
       round(100.0 * count(*) /
             (SELECT count(*) FROM workspace.tlc_silver.trips), 3) AS pct_of_clean_rows
FROM workspace.tlc_silver.trips_quarantine
GROUP BY _dq_rule
ORDER BY rows_quarantined DESC;