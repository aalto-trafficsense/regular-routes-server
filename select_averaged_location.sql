SELECT
device_id,
AVG(CAST(extract(epoch from time) as integer)) as time_stamp,
CAST(date_part('minute',time) as integer) as minute,
CAST(date_part('hour',time) as integer) as hour,
CAST(date_part('year',time) as integer) as year,
CAST(date_part('dow',time) as integer) as day_of_week,
CAST(date_part('doy',time) as integer) as day_of_year,
CAST(AVG(ST_Y(coordinate::geometry)) as float) as longitude,
CAST(AVG(ST_X(coordinate::geometry)) as float) as latitude,
CAST(AVG(accuracy) as float) as accuracy,
waypoint_id
FROM device_data
-- correct one is device_data
WHERE (time > current_timestamp - interval '120 minutes') 
GROUP BY device_id,year,day_of_year,hour,minute, device_data.time, device_data.waypoint_id
ORDER BY device_id, time_stamp
LIMIT 10
;


