\i create_average_table.sql

INSERT INTO averaged_location
    SELECT
        device_id,
        (date_trunc('minute', time) + interval '1 minutes') as time_stamp, -- time stamp to the nearest minute
        CAST(date_part('minute',time) as integer) as minute,
        CAST(date_part('hour',time) as integer) as hour,
        CAST(date_part('year',time) as integer) as year,
        CAST(date_part('dow',time) as integer) as day_of_week,
        CAST(date_part('doy',time) as integer) as day_of_year,
        CAST(AVG(ST_Y(coordinate::geometry)) as float) as longitude,
        CAST(AVG(ST_X(coordinate::geometry)) as float) as latitude,
        CAST(AVG(accuracy) as float) as accuracy
      FROM device_data
      GROUP BY device_id, time_stamp, minute, hour, year, day_of_week, day_of_year
      -- GROUP BY device_id, time_stamp
      ORDER BY device_id, time_stamp
;

