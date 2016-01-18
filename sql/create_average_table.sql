DROP TABLE IF EXISTS averaged_location;

CREATE TABLE averaged_location (
  device_id integer,
  time_stamp timestamp, -- was: integer
  minute integer,
  hour integer,
  year integer,
  day_of_week integer,
  day_of_year integer,
  longitude float,
  latitude float,
  accuracy float
  --waypoint_id bigint
);
