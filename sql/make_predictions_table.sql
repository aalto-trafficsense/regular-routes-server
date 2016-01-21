--
-- A table to store predictions
--

DROP TABLE IF EXISTS predictions;

CREATE TABLE predictions (
  device_id int,
  cluster_id bigint,
  prediction_time timestamp,
  offset_time integer,        -- for example, an offset_time of 1 indicates that the prediction refers to now() + 1 (we are dealing with minutes, so this means 1 minute ahead); -5 would mean 5 minutes in the past (not a prediction, but possibly used to make a prediction).
);

CREATE TABLE pred_temp AS 
	SELECT p.device_id, c.location FROM predictions as p, cluster_centers as p 
		WHERE p.device_id = c.device_id AND p.cluster_id = c.cluster_id
