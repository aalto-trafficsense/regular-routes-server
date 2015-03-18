UPDATE device_data
SET waypoint_id = snapping.waypoint_id,
    snapping_time = now()
FROM (
  SELECT device_data.id AS id, waypoint.id AS waypoint_id
  FROM device_data
  LEFT JOIN LATERAL (
      SELECT osm_id
      FROM roads
      WHERE ST_DWithin(roads.geo, coordinate, 100)
      ORDER BY ST_Distance(roads.geo, coordinate) ASC
      LIMIT 1
  ) AS road ON true
  LEFT JOIN LATERAL (
      SELECT id
      FROM roads_waypoints
      JOIN waypoints
      ON waypoint_id = waypoints.id
      WHERE road_id = road.osm_id
      ORDER BY ST_Distance(waypoints.geo, coordinate) ASC
      LIMIT 1
  ) AS waypoint ON true
  WHERE snapping_time IS NULL
) AS snapping
WHERE device_data.id = snapping.id
