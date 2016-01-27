--
-- A table to store predictions
--

DROP TABLE IF EXISTS predictions;

CREATE TABLE predictions (
  device_id int,
  cluster_id bigint,
  time_stamp timestamp,
  time_index integer        
); -- for example, an time_index of 1 indicates that the prediction refers to time_stamp + 1 (we are dealing with minutes, so this means 1 minute ahead); -5 would mean 5 minutes in the past (not a prediction, but possibly used to make a prediction).

--CREATE TABLE pred_temp AS 
--	SELECT p.device_id, c.location FROM predictions as p, cluster_centers as p 
--		WHERE p.device_id = c.device_id AND p.cluster_id = c.cluster_id
