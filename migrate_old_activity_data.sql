-- WARNING: This script is NOT enough to migrate an old database completely

UPDATE device_data
  SET
    activity_1 = values.activity_types[1],
    activity_2 = values.activity_types[2],
    activity_3 = values.activity_types[3],
    activity_1_conf = values.confidences[1],
    activity_2_conf = values.confidences[2],
    activity_3_conf = values.confidences[3]
  FROM (
    SELECT DISTINCT ON (device_data_id)
      device_data_id,
      array_agg(activity_type::activity_type_enum) OVER device_data_point AS activity_types,
      array_agg(confidence)    OVER device_data_point AS confidences
    FROM
      activity_data
    JOIN activity_type
    ON activity_type.id = activity_type_id
    WINDOW device_data_point AS (
      PARTITION BY device_data_id ORDER BY ordinal
      RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    )
  ) AS values
  WHERE id = values.device_data_id
;
