SELECT * FROM 
    (SELECT device_id, COUNT(*) AS num_records, EXTRACT(EPOCH FROM (max(time) - min(time)))/60/60/24 AS num_days FROM device_data GROUP BY device_id) as tbl 
    WHERE tbl.num_records > 5000 and tbl.num_days > 3 ORDER BY device_id;
