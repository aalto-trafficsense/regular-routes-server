DELETE FROM device_data
WHERE id IN (
  SELECT id
  FROM (
    SELECT id, row_number() OVER point AS row
    FROM device_data
    WINDOW point AS (PARTITION BY device_id, time)
  ) AS grouped_points
  WHERE row > 1
);
