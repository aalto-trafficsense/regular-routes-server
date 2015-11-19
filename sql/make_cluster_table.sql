DROP TABLE IF EXISTS cluster_centers;

CREATE TABLE cluster_centers (
  device_id int,
  cluster_id bigint,
  longitude float,
  latitude float,
  time_stamp timestamp, -- the time when the clustering was last carried out
  location geography(Point,4326)
);


