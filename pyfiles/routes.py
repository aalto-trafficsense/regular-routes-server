#!/usr/bin/env python

from collections import Counter

from sqlalchemy.sql import and_, literal_column, select
from sqlalchemy.types import Float

from pyfiles.common_helpers import do_cluster, group_unsorted


def get_routes(db, threshold, user, start=None, end=None):
    ends = db.metadata.tables["leg_ends"]
    legs = db.metadata.tables["leg_modes"]
#    lwps = db.metadata.tables["leg_waypoints"]
    dd = db.metadata.tables["device_data"]
    trips = db.metadata.tables["trips"]
    places = db.metadata.tables["places"]

    # Fetch trips
    oleg = legs.alias("oleg")
    dleg = legs.alias("dleg")
    oend = ends.alias("oend")
    dend = ends.alias("dend")
    oplace = places.alias("oplace")
    dplace = places.alias("dplace")
    sel = select([
            trips.c.id,
            oplace.c.label.label("origin"),
            dplace.c.label.label("destination")]) \
        .select_from(trips
            .join(oleg, trips.c.origin == oleg.c.id) \
            .join(oend, oleg.c.cluster_start == oend.c.id) \
            .join(oplace, oend.c.place == oplace.c.id) \
            .join(dleg, trips.c.destination == dleg.c.id) \
            .join(dend, dleg.c.cluster_start == dend.c.id) \
            .join(dplace, dend.c.place == dplace.c.id)) \
        .where(oend.c.user_id == user)
    tripitems = {x[0]: dict(x) for x in db.engine.execute(sel)}

    # Add waypointmodes on trips
    sel = select(
        [   legs.c.trip,
            legs.c.mode,
            literal_column(
                """st_x(st_snaptogrid(coordinate::geometry, .0036 / cos(radians(
                      st_y(st_snaptogrid(coordinate::geometry, .0036))))))"""),
            literal_column(
                'st_y(st_snaptogrid(coordinate::geometry, .0036))')]) \
        .select_from(legs.join(dd, and_(
            dd.c.device_id == legs.c.device_id,
            dd.c.time.between(legs.c.time_start, legs.c.time_end)))) \
        .where(legs.c.trip.isnot(None)) \
        .where(legs.c.user_id == user) \
        .distinct() \
        .order_by(legs.c.trip)
    if start:
        sel = sel.where(legs.c.time_end > start)
    if end:
        sel = sel.where(legs.c.time_start < end)

    trippoints = group_unsorted(db.engine.execute(sel), lambda x: x.trip)
    for tid, titem in list(tripitems.items()):
        # No time limit on tripitems query so drop those without points now
        points = trippoints.get(tid, [])
        if not points:
            del tripitems[tid]
        # Strip trip and convert Decimal to float for json serialization
        titem["points"] = [(x[1], float(x[2]), float(x[3])) for x in points]

    # Group by origin/destination
    odgroups = group_unsorted(
       iter(tripitems.values()), lambda x: (x["origin"], x["destination"]))

    # Cluster within OD groups by waypoint/mode similarity
    def route_merge(*routes):
        total = 0
        weights = Counter()
        for route in routes:
            trips = len(route["trips"])
            total += trips
            for point, prob in route["probs"].items():
                weights[point] += trips * prob
        return {
            "probs": {p: 1.0*c/total for p, c in weights.items()},
            "trips": sum((x["trips"] for x in routes), [])}

    def proto_distance(p0, p1):
        p0, p1 = p0["probs"], p1["probs"]
        keys = set(list(p0.keys()) + list(p1.keys()))
        union = sum(max(p0.get(x, 0), p1.get(x, 0)) for x in keys)
        intersection = sum(min(p0.get(x, 0), p1.get(x, 0)) for x in keys)
        return 1.0 - 1.0 * intersection / union if union else 1

    counter = Counter()
    for od, group in odgroups.items():
        clusters = [
            {   "probs": Counter({p: 1 for p in x.get("points", [])}),
                "trips": [x]}
            for x in group]
        clustered = do_cluster(
            clusters, route_merge, proto_distance, threshold)
        for c in clustered:
            counter["intraclusters"] += 1
            counter["trips"] += len(c["trips"])
        counter["odclusters"] += 1
        yield od, clustered
