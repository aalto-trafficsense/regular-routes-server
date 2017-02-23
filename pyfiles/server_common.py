import json

from collections import namedtuple
from csv import DictWriter
from datetime import datetime, timedelta
from itertools import groupby
from StringIO import StringIO

from flask import abort, make_response
from sqlalchemy.sql import and_, cast, func, select
from sqlalchemy.types import String

from pyfiles.common_helpers import group_unsorted, mode_str
from pyfiles.routes import get_routes

from pyfiles.database_interface import (
    mass_transit_types, update_user_distances)

def common_trips_rows(request, db, user):
    firstday = request.args.get("firstday")
    firstday = firstday and datetime.strptime(firstday, '%Y-%m-%d')
    firstday = firstday or datetime.now()

    lastday = request.args.get("lastday")
    lastday = lastday and datetime.strptime(lastday, '%Y-%m-%d')
    lastday = lastday or firstday

    date_start = firstday.replace(hour=0, minute=0, second=0, microsecond=0)
    date_end = lastday.replace(hour=0, minute=0, second=0, microsecond=0) \
        + timedelta(hours=24)

    legs = db.metadata.tables["leg_modes"]
    where = and_(
        legs.c.user_id == int(user),
        legs.c.time_end >= date_start,
        legs.c.time_start <= date_end)

    return date_start, fetch_legs(db, where)


def fetch_legs(db, where):
    legs = db.metadata.tables["leg_modes"]
    legends0 = db.metadata.tables["leg_ends"]
    legends1 = legends0.alias("legends1")
    places0 = db.metadata.tables["places"]
    places1 = places0.alias("places1")

    s = select(
        [   legs.c.id,
            legs.c.time_start,
            legs.c.time_end,
            legs.c.activity,
            legs.c.line_type,
            legs.c.line_name,
            legs.c.km,
            legs.c.trip,
            func.coalesce(places0.c.label, cast(
                places0.c.id, String)).label("startplace"),
            func.coalesce(places1.c.label, cast(
                places1.c.id, String)).label("endplace")],
        where,
        legs.outerjoin(legends0, legs.c.cluster_start == legends0.c.id) \
            .outerjoin(legends1, legs.c.cluster_end == legends1.c.id) \
            .outerjoin(places0, legends0.c.place == places0.c.id) \
            .outerjoin(places1, legends1.c.place == places1.c.id),
        order_by=legs.c.time_start)

    return db.engine.execute(s)


def common_trips_csv(request, db, user):
    # Sadly, both the csv source and the response sink want to be the driver,
    # so not buffering the whole thing isn't convenient
    buf = StringIO()
    _, rows = common_trips_rows(request, db, user)
    print rows.keys()
    csv = DictWriter(buf, rows.keys())
    csv.writeheader()
    for r in rows:
        r = {k: v.encode("utf8") if isinstance(v, unicode) else v
             for k, v in r.items()}
        csv.writerow(r)

    response = make_response(buf.getvalue())
    response.mimetype = "text/csv"
    return response


def common_routes_json(request, db, user):
    clustered = list(get_routes(db, .5, user))
    for od, routes in clustered:
        for route in routes:
            print "probs", route["probs"]
            for trip in route["trips"]:
                print "trip", trip["id"]

    tids = [
        trip["id"]
        for od, routes in clustered
        for route in routes
        for trip in route["trips"]]

    legs = db.metadata.tables["leg_modes"]
    where = legs.c.trip.in_(tids)
    legrows = fetch_legs(db, where)
    bytrip = group_unsorted(legrows, lambda x: x.trip)

    # Sort most common route pattern in first
    clustered.sort(key=lambda x: -sum(len(y["trips"]) for y in x[1]))

    for od, routes in clustered:
        # Sort most common OD first
        routes.sort(key=lambda x: -len(x["trips"]))
        for route in routes:
            # Sort oldest trip first
            route["trips"].sort(key=lambda x: bytrip[x["id"]][0].time_start)
            # Make probs jsonifiable, tuple keys aren't so
            route["probs"] = route["probs"].items()

    bytrip = {
        x: {"date": y[0].time_start.strftime("%Y-%m-%d"),
            "render": render_trips_day(y)}
        for x, y in bytrip.iteritems()}

    return json.dumps({
        "clustered": clustered,
        "trips": bytrip})


def common_trips_json(request, db, user):
    date_start, rows = common_trips_rows(request, db, user)

    # A leg overlapping the midnight at start of range is fudged into the first
    # day. It's mostly relevant in single day view
    dategrouped = groupby(
        rows,
        lambda x: max(x.time_start, date_start) \
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .strftime("%Y-%m-%d"))

    return json.dumps([{
            "date": date,
            "data": render_trips_day(legrows)}
        for date, legrows in dategrouped])


def render_trips_day(legrows):

    def fmt_duration(t0, t1):
        h, m = divmod(int(round((t1 - t0).total_seconds() / 60)), 60)
        return h and "{}h{:02}".format(h, m) or "{}min".format(m)

    def fmt_distance(km):
        if km >= 9.95:
            return "%.0fkm" % km
        if km >= 0.05:
            return "%.1fkm" % km
        return ""

    State = namedtuple("State", ["time", "activity", "place"])
    steps = []
    def step(**kw):
        prior = steps and steps[-1] or State(None, None, None)
        steps.append(prior._replace(**kw))

    strings = (
        (   x.id,
            str(x.time_start)[11:16],
            str(x.time_end)[11:16],
            mode_str(x.activity, x.line_type, x.line_name),
            " ".join([
                fmt_duration(x.time_start, x.time_end), fmt_distance(x.km)]),
            x.startplace,
            x.endplace or x.startplace)
        for x in legrows)

    pt1str = pactivity = pplace1 = None
    for legid, t0str, t1str, activity, duration, place0, place1 in strings:

        # Emit prior end time with appropriate aligment based on change
        if pt1str and t0str != pt1str:
            step(time=(pt1str, "end"))

        # If place changed, or transfer, emit prior place on prior leg
        if pplace1 and (place0 != pplace1 or activity != "STILL"):
            step(place=pplace1)

        # Visualize time gap if place changes, or activity is same
        if pt1str and t0str != pt1str and (
                (pactivity and pactivity == activity)
                or (pplace1 and pplace1 != place0)):
            step(time=None, activity=None, place=None)

        # If going from stop to move starting from same but with internal
        # location change, terminate place label and use activity there instead
        timecell = (t0str, t0str == pt1str and "both" or "start")
        actcell = (activity, duration, legid)
        placecell = place0
        if (pactivity == "STILL" and activity != "STILL"
                and pplace1 == place0 and pplace1 != place1):
            placecell = False
        step(time=timecell, activity=actcell, place=placecell)

        # Needed for move with internal place change and gap on both sides
        if place1 and place0 != place1:
            step(place=False)

        pt1str, pactivity, pplace1 = t1str, activity, place1

    if pt1str:
        step(time=(pt1str, "end"))
    if pplace1:
        step(place=pplace1)

    pivot = zip(*steps)
    rle = [[(y, len(list(z))) for y, z in groupby(x)] for x in pivot]
    named = zip(State._fields, rle)

    return named


def common_setlegmode(request, db, user):
    """Allow user to correct detected transit modes and line names."""

    data = request.json
    legid = data["id"]
    legact = data["activity"]
    legline = data.get("line_name") or None

    # Ignore line name given with non-transit mode
    if legact not in mass_transit_types:
        legline = None

    devices = db.metadata.tables["devices"]
    users = db.metadata.tables["users"]
    legs = db.metadata.tables["legs"]
    modes = db.metadata.tables["modes"]

    # Get existing leg, while verifying that the user matches appropriately
    leg = db.engine.execute(select(
        [legs.c.device_id, legs.c.time_start, legs.c.time_end],
        and_(devices.c.user_id == user, legs.c.id == legid),
        from_obj=devices.join(users).join(legs))).first()
    if not leg:
        abort(403)

    values = {"leg": legid, "source": "USER", "mode": legact, "line": legline}
    existing = db.engine.execute(modes.select().where(and_(
        modes.c.leg == legid, modes.c.source == "USER"))).first()
    if existing:
        where = modes.c.id == existing.id
        if legact is None:
            db.engine.execute(modes.delete().where(where))
        else:
            db.engine.execute(modes.update().where(where).values(values))
    elif legact is not None:
        db.engine.execute(modes.insert().values(values))

    # Recalculate distances
    update_user_distances(user, leg.time_start, leg.time_end)

    return leg.device_id, legid, legact, legline
