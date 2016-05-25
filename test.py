#!/usr/bin/env python

import datetime, sys

from sqlalchemy.sql import select, and_, func

from pyfiles.constants import DEST_DURATION_MIN, DEST_RADIUS_MAX
from pyfiles.common_helpers import trace_regular_destinations
from scheduler import db

start = datetime.datetime(2016, 4, 1)
end = datetime.datetime(2016, 6, 1)


def user_regular_destinations(user):

# filtered_data = db.metadata.tables["device_data_filtered"]
# query = select(
#         [   func.ST_AsGeoJSON(filtered_data.c.coordinate).label("geojson"),
#             filtered_data.c.activity,
#             filtered_data.c.time],
#         and_(
#             filtered_data.c.user_id == user,
#             filtered_data.c.time >= start,
#             filtered_data.c.time <= end),
#         filtered_data)

    device_data = db.metadata.tables["device_data"]
    devices = db.metadata.tables["devices"]

# device = nnn
# query = select(
#         [   func.ST_AsGeoJSON(device_data.c.coordinate).label("geojson"),
#             device_data.c.accuracy,
#             device_data.c.time],
#         and_(
#             device_data.c.device_id == device,
#             device_data.c.time >= start,
#             device_data.c.time <= end),
#         device_data,
#         order_by=device_data.c.time)
# points = db.engine.execute(query).fetchall()

    query = select(
        [   func.ST_AsGeoJSON(device_data.c.coordinate).label("geojson"),
            device_data.c.accuracy,
            device_data.c.time],
        and_(
            devices.c.user_id == user,
            device_data.c.time >= start,
            device_data.c.time <= end),
        device_data.join(devices),
        order_by=device_data.c.time)
    points = db.engine.execute(query)

    dests = trace_regular_destinations(
        points, DEST_RADIUS_MAX, DEST_DURATION_MIN, 0)
    print type(dests), len(dests)
    print type(dests[0]), len(dests[0]), dests[0].keys()


if __name__ == "__main__":
    user_regular_destinations(int(sys.argv[1]))
