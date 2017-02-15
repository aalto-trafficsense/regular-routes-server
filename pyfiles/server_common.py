import json

from collections import namedtuple
from datetime import datetime, timedelta
from itertools import groupby
from sqlalchemy import and_, cast, func, select, String

from pyfiles.common_helpers import mode_str


def common_trips_json(request, db, user):
    date_start = 'date' in request.args \
        and datetime.strptime(request.args['date'], '%Y-%m-%d') \
        or datetime.now()

    date_start = date_start.replace(hour=0, minute=0, second=0, microsecond=0)
    date_end = date_start + timedelta(hours=24)

    legs = db.metadata.tables["leg_modes"]
    legends0 = db.metadata.tables["leg_ends"]
    legends1 = legends0.alias("legends1")
    places0 = db.metadata.tables["places"]
    places1 = places0.alias("places1")

    s = select(
        [   legs.c.time_start,
            legs.c.time_end,
            legs.c.activity,
            legs.c.line_type,
            legs.c.line_name,
            legs.c.km,
            func.coalesce(places0.c.label, cast(
                places0.c.id, String)).label("startplace"),
            func.coalesce(places1.c.label, cast(
                places1.c.id, String)).label("endplace")],
        and_(
            legs.c.user_id == int(user),
            legs.c.time_end >= date_start,
            legs.c.time_start <= date_end),
        legs.outerjoin(legends0, legs.c.cluster_start == legends0.c.id) \
            .outerjoin(legends1, legs.c.cluster_end == legends1.c.id) \
            .outerjoin(places0, legends0.c.place == places0.c.id) \
            .outerjoin(places1, legends1.c.place == places1.c.id),
        order_by=legs.c.time_start)

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
        (   str(t0)[11:16],
            str(t1)[11:16],
            mode_str(activity, ltype, lname),
            " ".join([fmt_duration(t0, t1), fmt_distance(km)]),
            pi0,
            pi1 or pi0)
        for t0, t1, activity, ltype, lname, km, pi0, pi1
        in db.engine.execute(s))

    pt1str = pactivity = pplace1 = None
    for t0str, t1str, activity, duration, place0, place1 in strings:

        # Emit prior end time with appropriate aligment based on change
        if pt1str and t0str != pt1str:
            step(time=(pt1str, "end"))

        # If place changed, or transfer, emit prior place on prior leg
        if pplace1 and place0 != pplace1 or activity != "STILL":
            step(place=pplace1)

        # Visualize time gap if place changes, or activity is same
        if pt1str and t0str != pt1str and (
                (pactivity and pactivity == activity)
                or (pplace1 and pplace1 != place0)):
            step(time=None, activity=None, place=None)

        # If going from stop to move starting from same but with internal
        # location change, terminate place label and use activity there instead
        timecell = (t0str, t0str == pt1str and "both" or "start")
        actcell = (activity, duration)
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

    dated = [{
        "date": date_start.strftime("%Y-%m-%d"),
        "data": named}]

    return json.dumps(dated)
