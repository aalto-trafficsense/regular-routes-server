DROP TABLE IF EXISTS predictions;

CREATE TABLE predictions (
  device_id int,
  cluster_id bigint,
  prediction_time timestamp,
  offset_time integer,        -- in seconds (offset wrt prediction time); negative numbers indicate input/history, positive numbers indicate output/future
);

CREATE TABLE pred_temp AS 
	SELECT p.device_id, c.location FROM predictions as p, cluster_centers as p 
		WHERE p.device_id = c.device_id AND p.cluster_id = c.cluster_id
