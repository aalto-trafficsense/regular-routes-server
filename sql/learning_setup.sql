
DROP TABLE IF EXISTS averaged_location;

CREATE TABLE averaged_location (
  device_id integer,
  time_stamp integer,
  minute integer,
  hour integer,
  year integer,
  day_of_week integer,
  day_of_year integer,
  longitude float,
  latitude float,
  accuracy float,
  waypoint_id bigint
);

INSERT INTO averaged_location
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
      GROUP BY device_id,year,day_of_year,hour,minute, device_data.time, device_data.waypoint_id
      ORDER BY device_id, time_stamp
;

DROP TABLE IF EXISTS cluster_centers;

CREATE TABLE cluster_centers (
  device_id int,
  cluster_id bigint,
  longitude float,
  latitude float,
  time_stamp timestamp, -- the time when the clustering was last carried out
  location geography(Point,4326)
);

DROP TABLE IF EXISTS predictions;

CREATE TABLE predictions (
  device_id int,
  cluster_id bigint,
  time_stamp timestamp,
  time_index integer
);

CREATE TABLE pred_temp AS 
	SELECT p.device_id, c.location FROM predictions as p, cluster_centers as p 
		WHERE p.device_id = c.device_id AND p.cluster_id = c.cluster_id
