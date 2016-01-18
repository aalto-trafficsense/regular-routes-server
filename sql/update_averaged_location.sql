-- Update

INSERT INTO averaged_location 
    SELECT
	device_id,
	(date_trunc('minute', time) + interval '1 minutes') as time_stamp,
	--AVG(CAST(extract(epoch from time) as integer)) as time_stamp,
	CAST(date_part('minute',time) as integer) as minute,
	CAST(date_part('hour',time) as integer) as hour,
	CAST(date_part('year',time) as integer) as year,
	CAST(date_part('dow',time) as integer) as day_of_week,
	CAST(date_part('doy',time) as integer) as day_of_year,
	CAST(AVG(ST_Y(coordinate::geometry)) as float) as longitude,
	CAST(AVG(ST_X(coordinate::geometry)) as float) as latitude,
	CAST(AVG(accuracy) as float) as accuracy
	-- waypoint_id
	FROM device_data
	-- WHERE (time > current_timestamp - interval '5 minutes') 
	WHERE (time > (SELECT MAX(time_stamp) FROM averaged_location)) 
	-- AND (time < date_trunc('minute', (SELECT max(time_stamp) FROM device_data)))
	-- GROUP BY device_id,year,day_of_year,hour,minute, device_data.time, device_data.waypoint_id
	GROUP BY device_id, time_stamp, minute, hour, year, day_of_week, day_of_year
	ORDER BY device_id, time_stamp
	;

-- LIMIT 10

