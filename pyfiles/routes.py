#!/usr/bin/env python

from collections import Counter
from itertools import chain, groupby

from sqlalchemy.sql import select

from pyfiles.common_helpers import do_cluster


def group_unsorted(iterable, keyfunc):
    r = dict()
    for x in iterable:
        r.setdefault(keyfunc(x), list()).append(x)
    return r


def test(db, threshold):
    ends = db.metadata.tables["leg_ends"]
    legs = db.metadata.tables["leg_modes"]
    lwps = db.metadata.tables["leg_waypoints"]
    trips = db.metadata.tables["trips"]

    # Fetch trips
    oleg = legs.alias("oleg")
    dleg = legs.alias("dleg")
    oend = ends.alias("oend")
    dend = ends.alias("dend")
    sel = select([
            trips.c.id,
            oend.c.place.label("origin"),
            dend.c.place.label("destination")]) \
        .select_from(trips
            .join(oleg, trips.c.origin == oleg.c.id) \
            .join(oend, oleg.c.cluster_start == oend.c.id) \
            .join(dleg, trips.c.destination == dleg.c.id) \
            .join(dend, dleg.c.cluster_start == dend.c.id)) \
        .where(oend.c.user_id == 16)
    tripitems = {x[0]: dict(x) for x in db.engine.execute(sel)}

    # Add waypointmodes on trips
    sel = select([legs.c.trip, legs.c.mode, lwps.c.waypoint]) \
        .select_from(legs.join(lwps, lwps.c.leg == legs.c.id)) \
        .where(legs.c.trip.isnot(None)) \
        .where(legs.c.user_id == 16) \
        .order_by(legs.c.trip)
    trippoints = groupby(db.engine.execute(sel), lambda x: x.trip)
    for trip, points in trippoints:
        if trip not in tripitems:
            continue
        tripitems[trip]["points"] = list(x[1:] for x in points) # strip trip

    # Group by origin/destination
    odgroups = group_unsorted(
       tripitems.itervalues(), lambda x: (x["origin"], x["destination"]))

    # Cluster within OD groups by waypoint/mode similarity
    def route_merge(*routes):
        total = 0
        weights = Counter()
        for route in routes:
            trips = len(route["trips"])
            total += trips
            for point, prob in route["probs"].iteritems():
                weights[point] += trips * prob
        return {
            "probs": {p: 1.0*c/total for p, c in weights.iteritems()},
            "trips": sum((x["trips"] for x in routes), [])}

    def proto_distance(p0, p1):
        p0, p1 = p0["probs"], p1["probs"]
        keys = set(p0.keys() + p1.keys())
        union = sum(max(p0.get(x, 0), p1.get(x, 0)) for x in keys)
        intersection = sum(min(p0.get(x, 0), p1.get(x, 0)) for x in keys)
        return 1.0 - 1.0 * intersection / union

    counter = Counter()
    for _, group in odgroups.iteritems():
        clusters = [
            {   "probs": Counter({p: 1 for p in x["points"]}),
                "trips": [x]}
            for x in group]
        clustered = do_cluster(
            clusters, route_merge, proto_distance, threshold)
        for c in clustered:
            counter["intraclusters"] += 1
            counter["trips"] += len(c["trips"])
        counter["odclusters"] += 1
    print counter
