import json
import tempfile
import zipfile

from collections import namedtuple
from csv import DictWriter
from datetime import datetime, timedelta
from itertools import groupby
from io import StringIO

from flask import abort, jsonify, make_response

from sqlalchemy.sql import (
    and_, between, cast, func, literal, not_, or_, select, text)

from sqlalchemy.types import String

from pyfiles.common_helpers import (
    dict_groups,
    group_unsorted,
    mode_str,
    simplify_geometry,
    trace_discard_sidesteps,
    trace_linestrings)

from pyfiles.constants import BAD_LOCATION_RADIUS
from pyfiles.routes import get_routes

from pyfiles.database_interface import (
    mass_transit_types, update_user_distances, get_csv, get_device_table_ids)

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
    print(list(rows.keys()))
    csv = DictWriter(buf, list(rows.keys()))
    csv.writeheader()
    for r in rows:
        r = {k: v.encode("utf8") if isinstance(v, str) else v
             for k, v in list(r.items())}
        csv.writerow(r)

    response = make_response(buf.getvalue())
    response.mimetype = "text/csv"
    return response


def common_routes_json(request, db, user):
    firstday = request.args.get("firstday")
    firstday = firstday and datetime.strptime(firstday, '%Y-%m-%d')
    firstday = firstday or datetime.now()

    lastday = request.args.get("lastday")
    lastday = lastday and datetime.strptime(lastday, '%Y-%m-%d')
    lastday = lastday or firstday

    date_start = firstday.replace(hour=0, minute=0, second=0, microsecond=0)
    date_end = lastday.replace(hour=0, minute=0, second=0, microsecond=0) \
        + timedelta(hours=24)

    clustered = list(get_routes(db, 1.0/3, user, date_start, date_end))
    for od, routes in clustered:
        for route in routes:
            print("probs", route["probs"])
            for trip in route["trips"]:
                print("trip", trip["id"])

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
            route["probs"] = list(route["probs"].items())

    bytrip = {
        x: {"date": y[0].time_start.strftime("%Y-%m-%d"),
            "render": render_trips_day(y)}
        for x, y in bytrip.items()}

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
        if km is None: km = 0.0
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

    pivot = list(zip(*steps))
    rle = [[(y, len(list(z))) for y, z in groupby(x)] for x in pivot]
    named = list(zip(State._fields, rle))

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


def common_path(request, db, where):
    dd = db.metadata.tables["device_data"]
    devices = db.metadata.tables["devices"]
    legs = db.metadata.tables["leg_modes"]
    users = db.metadata.tables["users"]

    # get data for specified date, or last 12h if unspecified
    date = request.args.get("date")

    # passed on to simplify_geometry
    maxpts = int(request.args.get("maxpts") or 0)
    mindist = int(request.args.get("mindist") or 0)

    # Exclude given comma-separated modes in processed path of path, stops by
    # default. Blank argument removes excludes
    exarg = request.args.get("exclude")
    exclude = True if exarg == "" else not_(
        legs.c.mode.in_((exarg or "STILL").split(",")))

    if date:
        start = datetime.strptime(date, '%Y-%m-%d').replace(
            hour=0, minute=0, second=0, microsecond=0)
    else:
        start = datetime.now() - timedelta(hours=12)
    end = start + timedelta(hours=24)

    # in the export link case, we get a date range
    firstday = request.args.get("firstday")
    firstday = firstday and datetime.strptime(firstday, '%Y-%m-%d')
    firstday = firstday or datetime.now()

    lastday = request.args.get("lastday")
    lastday = lastday and datetime.strptime(lastday, '%Y-%m-%d')
    lastday = lastday or firstday

    date_start = firstday.replace(hour=0, minute=0, second=0, microsecond=0)
    date_end = lastday.replace(hour=0, minute=0, second=0, microsecond=0) \
        + timedelta(hours=24)

    if request.args.get("firstday") or request.args.get("lastday"):
        start, end = date_start, date_end

    # find end of user legs
    legsend = select(
        [func.max(legs.c.time_end).label("time_end")],
        where,
        devices.join(users).join(legs)).alias("legsend")

    # use user legs if available
    legsed = select(
        [   func.ST_AsGeoJSON(dd.c.coordinate).label("geojson"),
            cast(legs.c.mode, String).label("activity"),
            legs.c.line_name,
            legs.c.time_start.label("legstart"),
            cast(legs.c.time_start, String).label("time_start"),
            cast(legs.c.time_end, String).label("time_end"),
            legs.c.id,
            dd.c.time],
        and_(
            where,
            legs.c.activity != None,
            exclude,
            dd.c.time >= start,
            dd.c.time < end),
        devices \
            .join(users) \
            .join(legs) \
            .join(dd, and_(
                legs.c.device_id == dd.c.device_id,
                between(dd.c.time, legs.c.time_start, legs.c.time_end))))

    # fall back on raw trace beyond end of user legs
    unlegsed = select(
        [   func.ST_AsGeoJSON(dd.c.coordinate).label("geojson"),
            cast(dd.c.activity_1, String).label("activity"),
            literal(None).label("line_name"),
            literal(None).label("legstart"),
            literal(None).label("time_start"),
            literal(None).label("time_end"),
            literal(None).label("id"),
            dd.c.time],
        and_(
            where,
            dd.c.time >= start,
            dd.c.time < end,
            or_(legsend.c.time_end.is_(None), dd.c.time > legsend.c.time_end)),
        dd.join(devices).join(legsend, literal(True)))

    # Sort also by leg start time so join point repeats adjacent to correct leg
    query = legsed.union_all(unlegsed).order_by(text("time, legstart"))
    query = query.limit(35000) # sanity limit vs date range
    points = db.engine.execute(query)

    # re-split into legs, and the raw part
    segments = (
        legpts for (legid, legpts) in dict_groups(points, ["legstart"]))

    features = []
    for points in segments:
        # discard the less credible location points
        points = trace_discard_sidesteps(points, BAD_LOCATION_RADIUS)

        # simplify the path geometry by dropping redundant points
        points = simplify_geometry(
            points, maxpts=maxpts, mindist=mindist, keep_activity=True)

        features += trace_linestrings(points, (
            'id', 'activity', 'line_name', 'time_start', 'time_end'))

    return jsonify({'type': 'FeatureCollection', 'features': features})


def common_download_zip(user):
    # Adapted from: https://stackoverflow.com/a/11971561/5528498
    dev_ids = 'device_id in (' + get_device_table_ids(user) + ')'
    user_id = str(user)
    with tempfile.SpooledTemporaryFile() as tmp:
        with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as archive:
            archive.writestr('users.csv', get_csv('SELECT id,register_timestamp FROM users WHERE id='+user_id))
            archive.writestr('devices.csv', get_csv('SELECT id,user_id,device_model,created,last_activity,client_version FROM devices WHERE user_id='+user_id))
            archive.writestr('client_log.csv', get_csv('SELECT * FROM client_log WHERE user_id='+user_id))
            archive.writestr('device_alerts.csv', get_csv('SELECT * FROM device_alerts WHERE '+dev_ids))
            archive.writestr('device_data.csv', get_csv('SELECT * FROM device_data WHERE '+dev_ids))
            archive.writestr('device_data_filtered.csv', get_csv('SELECT * FROM device_data_filtered WHERE user_id='+user_id))
            archive.writestr('leg_ends.csv', get_csv('SELECT * FROM leg_ends WHERE user_id='+user_id))
            archive.writestr('leg_modes.csv', get_csv('SELECT * FROM leg_modes WHERE user_id='+user_id))
            archive.writestr('legs.csv', get_csv('SELECT * FROM legs WHERE user_id='+user_id))
            archive.writestr('travelled_distances.csv', get_csv('SELECT * FROM travelled_distances WHERE user_id='+user_id))

        # Reset file pointer
        tmp.seek(0)

        # Write file data to response
        response = make_response(tmp.read())
        response.mimetype = 'application/x-zip-compressed'
        return response
