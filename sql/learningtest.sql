
DROP TABLE IF EXISTS averaged_location;

CREATE TABLE averaged_location (
  -- Comment
  device_id integer,
  time_stamp integer,
  minute integer,
  hour integer,
  year integer,
  day_of_week integer,
  day_of_year integer,
  longitude FLOAT,
  latitude FLOAT,
  accuracy FLOAT,
  waypoint_id bigint
);

INSERT INTO averaged_location
      SELECT
        device_id,
	CAST(time as INT) as time_stamp,
	CAST(date_part('minute',time) as integer) as minute,
	CAST(date_part('hour',time) as integer) as hour,
	CAST(date_part('year',time) as integer) as year,
	CAST(date_part('dow',time) as integer) as day_of_week,
	CAST(date_part('doy',time) as integer) as day_of_year,
        ST_Y(coordinate::geometry) as longitude,
        ST_X(coordinate::geometry) as latitude,
        accuracy,
        waypoint_id
      FROM device_data
      ORDER BY time ASC
;
