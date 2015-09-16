.header on
.separator ";"
-- IMPORT RAW CSV
.import '2_regularroutes-05082015.csv' data_raw
DROP TABLE IF EXISTS data_averaged;
-- CAST IDs TO INTEGERS AND SLICE THE TIMESTAMP INTO COMPONENTS: H(our), M(inute), DoY(day of year), Y(ear), DoW(day of week) and t(seconds since 1970)
CREATE table data_sliced AS SELECT CAST(device_id AS integer) as d_id, CAST(strftime('%s',datetime(time)) as integer) as t, CAST(strftime('%Y', time) as integer) as Y, cast(strftime('%j', time) as integer) as DoY, CAST(strftime('%H', time) as integer) as H, cast(strftime('%M', time) as integer) as M, CAST(ROUND(strftime('%w', time),0) as integer) as DoW, CAST(longitude as float) as lon, CAST(latitude as float) as lat, CAST(accuracy as float) as acc, CAST(waypoint_id as int) as w_id from data_raw;
-- AVERAGE DATA BY MINUTE, i.e., average LAT,LON at time H:M on the DoY-th day of the Y-th year.
CREATE table data_averaged AS SELECT d_id, AVG(t) AS t, M, H, DoW, DoY,AVG(lon) AS lon, AVG(lat) AS lat,AVG(acc) as acc, w_id FROM data_sliced GROUP BY d_id,Y,DoY,H,M ORDER BY d_id,t;
-- CLEAN UP
DROP TABLE IF EXISTS data_raw;
DROP TABLE IF EXISTS data_sliced;
SELECT * from data_averaged;
