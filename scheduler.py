import datetime
from datetime import timedelta
from itertools import chain

import json
import os
import re
import sys
import time
import requests
# import urllib.request, urllib.error, urllib.parse
import paho.mqtt.subscribe as subscribe
import paho.mqtt.client as mqtt
# import threading
import ssl


from pyfiles.database_interface import (
    init_db, data_points_by_user_id_after, device_data_delete_duplicates,
    device_data_waypoint_snapping, generate_rankings,
    hsl_alerts_insert, weather_forecast_insert, weather_observations_insert,
    traffic_disorder_insert, match_pubtrans_alert, match_pubtrans_alert_test,
    match_traffic_disorder, update_global_statistics, update_user_distances)

from pyfiles.push_messaging import push_ptp_alert  # push_ptp_pubtrans, push_ptp_traffic,
from pyfiles.push_messaging import PTP_TYPE_PUBTRANS, PTP_TYPE_DIGITRAFFIC
from pyfiles.device_data_filterer import DeviceDataFilterer

from pyfiles.common_helpers import (
    interpret_jore,
    pairwise,
    point_coordinates)

from pyfiles.constants import DEST_RADIUS_MAX, TRIP_STOP_DURATION

from pyfiles.information_services import (
    hsl_alert_request, fmi_forecast_request, fmi_observations_request, traffic_disorder_request)
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from sqlalchemy.sql.expression import (
    and_, column, desc, func, nullsfirst, or_, select, text)

import logging
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

SETTINGS_FILE_ENV_VAR = 'REGULARROUTES_SETTINGS'
CLIENT_SECRET_FILE_NAME = 'client_secrets.json'

# set settings dir from env.var for settings file. fallback dir is server.py file's parent dir
settings_dir_path = os.path.abspath(os.path.dirname(os.getenv(SETTINGS_FILE_ENV_VAR, os.path.abspath(__file__))))
CLIENT_SECRET_FILE = os.path.join(settings_dir_path, CLIENT_SECRET_FILE_NAME)

app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

env_var_value = os.getenv(SETTINGS_FILE_ENV_VAR, None)
if env_var_value is not None:
    print('loading settings from: "' + str(env_var_value) + '"')
    app.config.from_envvar(SETTINGS_FILE_ENV_VAR)
else:
    print('Environment variable "SETTINGS_FILE_ENV_VAR" was not defined -> using debug mode')
    # assume debug environment
    app.config.from_pyfile('regularroutes.cfg')
    app.debug = True

db, store = init_db(app)

users_table = db.metadata.tables['users']
devices_table = db.metadata.tables['devices']
device_data_table = db.metadata.tables['device_data']
device_data_filtered_table = db.metadata.tables['device_data_filtered']
travelled_distances_table = db.metadata.tables['travelled_distances']
mass_transit_data_table = db.metadata.tables['mass_transit_data']
global_statistics_table = db.metadata.tables['global_statistics']


def initialize():
    print("initialising scheduler")
    scheduler = BackgroundScheduler()
    scheduler.start()
    # scheduler.add_job(retrieve_hsl_data, "cron", second="*/30")
    run_daily_tasks()
    scheduler.add_job(run_hourly_tasks, "cron", minute=24)
    scheduler.add_job(run_daily_tasks, "cron", hour="3")
    scheduler.add_job(retrieve_transport_alerts, "cron", minute="*/5")
    scheduler.add_job(retrieve_weather_info, "cron", hour="6")
    # global mass_transit_thread
    # mass_transit_thread = threading.Thread(target=init_mass_transit_live_reception)
    # mass_transit_thread.start()
    init_mass_transit_live_reception()
    print("scheduler init done")


def run_daily_tasks():
    # The order is important.
    run_hourly_tasks()
    filter_device_data()
    generate_distance_data()
    generate_global_statistics()
    mass_transit_cleanup()


def run_hourly_tasks():
    delete_device_data_duplicates()
    generate_legs()
    set_device_data_waypoints()
    set_leg_waypoints()
    generate_trips()


def generate_legs(keepto=None, maxtime=None, repair=False):
    """Record legs from stops and mobile activity found in device telemetry.

    keepto -- keep legs before this time, except last two or so for restart
    maxtime -- process device data up to this time
    repair -- re-evaluate and replace all changed legs"""

    now = datetime.datetime.now()
    if not keepto:
        keepto = now
    if not maxtime:
        maxtime = now

    print("generate_legs up to", maxtime)

    dd = db.metadata.tables["device_data"]
    legs = db.metadata.tables["legs"]

    # Find first and last point sent from each device.
    devmax = select(
        [   dd.c.device_id,
            func.min(dd.c.time).label("firstpoint"),
            func.max(dd.c.time).label("lastpoint")],
        dd.c.time < maxtime,
        group_by=dd.c.device_id).alias("devmax")

    # The last recorded leg transition may be to phantom move that, given more
    # future context, will be merged into a preceding stop. Go back two legs
    # for the rewrite start point.

    # Due to activity summing window context and stabilization, and stop
    # entry/exit refinement, the first transition after starting the filter
    # process is not necessarily yet in sync with the previous run. Go back
    # another two legs to start the process.

    # (The window bounds expression is not supported until sqlalchemy 1.1 so
    # sneak it in in the order expression...)
    order = text("""time_start DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING""")
    rrlegs = select(
        [   legs.c.device_id,
            func.nth_value(legs.c.time_start, 4) \
                .over(partition_by=legs.c.device_id, order_by=order) \
                .label("rewind"),
            func.nth_value(legs.c.time_start, 2) \
                .over(partition_by=legs.c.device_id, order_by=order) \
                .label("rewrite")],
        and_(legs.c.activity != None, legs.c.time_start <= keepto),
        distinct=True).alias("rrlegs")

    # Find end of processed legs, including terminator for each device.
    lastleg = select(
        [legs.c.device_id, func.max(legs.c.time_end).label("time_end")],
        legs.c.time_start < keepto,
        group_by=legs.c.device_id).alias("lastleg")

    # If trailing points exist, start from rewind leg, or first point
    starts = select(
        [   devmax.c.device_id,
            func.coalesce(rrlegs.c.rewind, devmax.c.firstpoint),
            func.coalesce(rrlegs.c.rewrite, devmax.c.firstpoint)],
        or_(lastleg.c.time_end == None,
            devmax.c.lastpoint > lastleg.c.time_end),
        devmax \
            .outerjoin(rrlegs, devmax.c.device_id == rrlegs.c.device_id) \
            .outerjoin(lastleg, devmax.c.device_id == lastleg.c.device_id))

    # In repair mode, just start from the top.
    if repair:
        starts = select([
            devmax.c.device_id,
            devmax.c.firstpoint.label("rewind"),
            devmax.c.firstpoint.label("start")])

    starts = starts.order_by(devmax.c.device_id)
    for device, rewind, start in db.engine.execute(starts):
        query = select(
            [   func.ST_AsGeoJSON(dd.c.coordinate).label("geojson"),
                dd.c.accuracy,
                dd.c.time,
                dd.c.device_id,
                dd.c.activity_1, dd.c.activity_1_conf,
                dd.c.activity_2, dd.c.activity_2_conf,
                dd.c.activity_3, dd.c.activity_3_conf],
            and_(
                dd.c.device_id == device,
                dd.c.time >= rewind,
                dd.c.time < maxtime),
            order_by=dd.c.time)

        points = db.engine.execute(query).fetchall()

        print("d"+str(device), "resume", str(start)[:19], \
            "rewind", str(rewind)[:19], str(len(points))+"p")

        filterer = DeviceDataFilterer() # not very objecty rly
        lastend = None
        newlegs = filterer.generate_device_legs(points, start)

        for (prevleg, _), (leg, legmodes) in pairwise(
                chain([(None, None)], newlegs)):

          with db.engine.begin() as t:

            lastend = leg["time_end"]

            print(" ".join([
                "d"+str(device),
                str(leg["time_start"])[:19],
                str(leg["time_end"])[:19],
                leg["activity"]]), end=' ')

            # Adjust leg for db entry
            gj0 = leg.pop("geojson_start", None)
            gj1 = leg.pop("geojson_end", None)
            leg.update({
                "device_id": device,
                "coordinate_start": gj0 and func.ST_GeomFromGeoJSON(gj0),
                "coordinate_end": gj1 and func.ST_GeomFromGeoJSON(gj1)})

            # Deal with overlapping legs on rewind/repair
            legid = t.execute(select(
                [legs.c.id],
                and_(*(legs.c[c] == leg[c] for c in list(leg.keys()))))).scalar()
            if legid:
                print("-> unchanged", end=' ')
            else:
                overlapstart = prevleg and prevleg["time_end"] or start
                overlaps = [x[0] for x in t.execute(select(
                    [legs.c.id],
                    and_(
                        legs.c.device_id == leg["device_id"],
                        legs.c.time_start < leg["time_end"],
                        legs.c.time_end > overlapstart),
                    order_by=legs.c.time_start))]
                if overlaps:
                    legid, dels = overlaps[0], overlaps[1:]
                    t.execute(legs.update(legs.c.id == legid, leg))
                    print("-> update", end=' ')
                    if dels:
                        t.execute(legs.delete(legs.c.id.in_(dels)))
                        print("-> delete %d" % len(dels))
                else:
                    ins = legs.insert(leg).returning(legs.c.id)
                    legid = t.execute(ins).scalar()
                    print("-> insert", end=' ')

            # Delete mismatching modes, add new modes
            modes = db.metadata.tables["modes"]
            exmodes = {x[0]: x[1:] for x in t.execute(select(
                [modes.c.source, modes.c.mode, modes.c.line],
                legs.c.id == legid,
                legs.join(modes)))}
            for src in set(exmodes).union(legmodes):
                ex, nu = exmodes.get(src), legmodes.get(src)
                if nu == ex:
                    continue
                if ex is not None:
                    print("-> del", src, ex, end=' ')
                    t.execute(modes.delete(and_(
                        modes.c.leg == legid, modes.c.source == src)))
                if nu is not None:
                    print("-> ins", src, nu, end=' ')
                    t.execute(modes.insert().values(
                        leg=legid, source=src, mode=nu[0], line=nu[1]))

            print()

        # Emit null activity terminator leg to mark trailing undecided points,
        # if any, to avoid unnecessary reprocessing on resume.
        rejects = [x for x in points if not lastend or x["time"] > lastend]
        if rejects:
            db.engine.execute(legs.delete(and_(
                legs.c.device_id == device,
                legs.c.time_start <= rejects[-1]["time"],
                legs.c.time_end >= rejects[0]["time"])))
            db.engine.execute(legs.insert({
                "device_id": device,
                "time_start": rejects[0]["time"],
                "time_end": rejects[-1]["time"],
                "activity": None}))

    # Attach device legs to users.
    devices = db.metadata.tables["devices"]

    # Real legs from devices with the owner added in, also when unattached
    owned = select(
        [   devices.c.user_id.label("owner"),
            legs.c.id,
            legs.c.user_id,
            legs.c.time_start,
            legs.c.time_end],
        and_(legs.c.activity != None, legs.c.time_end < maxtime),
        devices.join(legs, devices.c.id == legs.c.device_id))

    detached = owned.where(legs.c.user_id.is_(None)).alias("detached")
    attached = owned.where(legs.c.user_id.isnot(None)).alias("attached")
    owned = owned.alias("owned")

    # Find most recently received leg attached per user
    maxattached = select(
        [attached.c.owner, func.max(attached.c.id).label("id")],
        group_by=attached.c.owner).alias("maxattached")

    # Find start of earliest unattached leg received later
    mindetached = select(
        [   detached.c.owner,
            func.min(detached.c.time_start).label("time_start")],
        or_(maxattached.c.id.is_(None), detached.c.id > maxattached.c.id),
        detached.outerjoin(
            maxattached, detached.c.owner == maxattached.c.owner),
        group_by=detached.c.owner).alias("mindetached")

    # Find start of attached overlapping leg to make it visible to the process
    overattached = select(
        [   attached.c.owner,
            func.min(attached.c.time_start).label("time_start")],
        from_obj=attached.join(mindetached, and_(
            attached.c.owner == mindetached.c.owner,
            attached.c.time_end > mindetached.c.time_start)),
        group_by=attached.c.owner).alias("overattached")

    # Find restart point
    starts = select(
        [   mindetached.c.owner,
            func.least(mindetached.c.time_start, overattached.c.time_start)],
        from_obj=mindetached.outerjoin(
            overattached, mindetached.c.owner == overattached.c.owner))

    # In repair mode, just start from the top.
    if repair:
        starts = select(
            [owned.c.owner, func.min(owned.c.time_start)],
            group_by=owned.c.owner)

    for user, start in db.engine.execute(starts.order_by(column("owner"))):
        # Ignore the special legacy user linking userless data
        if user == 0:
            continue

        print("u"+str(user), "start attach", start)

        # Get legs from user's devices in end time order, so shorter
        # legs get attached in favor of longer legs from a more idle device.
        s = select(
            [   owned.c.id,
                owned.c.time_start,
                owned.c.time_end,
                owned.c.user_id],
            and_(owned.c.owner == user, owned.c.time_start >= start),
            order_by=owned.c.time_end)

        lastend = None
        for lid, lstart, lend, luser in db.engine.execute(s):
            print(" ".join(["u"+str(user), str(lstart)[:19], str(lend)[:19]]), end=' ')
            if lastend and lstart < lastend:
                if luser is None:
                    print("-> detached")
                    continue
                db.engine.execute(legs.update(
                    legs.c.id==lid).values(user_id=None)) # detach
                print("-> detach")
                continue
            lastend = lend
            if luser == user:
                print("-> attached")
                continue
            db.engine.execute(legs.update(
                legs.c.id==lid).values(user_id=user)) # attach
            print("-> attach")

    # Cluster backlog in batches
    cluster_legs(1000)

    # Reverse geocode labels for places created or shifted by new legs
    label_places(60)


def cluster_legs(limit):
    """New leg ends and places are clustered live by triggers; this can be used
    to cluster data created earlier."""

    print("cluster_legs up to", limit)

    with db.engine.begin() as t:
        t.execute(text("SELECT legs_cluster(:limit)"), limit=limit)
        t.execute(text("SELECT leg_ends_cluster(:limit)"), limit=limit)


def label_places(timeout):
    """Add labels to places that have no labels, or position has shifted
    significantly since labeling.

    Reverse geocoding api url and rate limit are read from configuration, for
    example:

    REVERSE_GEOCODING_URI_TEMPLATE = 'https://search.mapzen.com/v1/reverse?api_key=API_KEY&sources=osm&size=20&point.lat={lat}&point.lon={lon}'
    REVERSE_GEOCODING_QUERIES_PER_SECOND = 6"""

    print("label_places up to %ds" % timeout)

    url_template = app.config.get('REVERSE_GEOCODING_URI_TEMPLATE')
    qps = app.config.get('REVERSE_GEOCODING_QUERIES_PER_SECOND')

    if None in (url_template, qps):
        log.info("REVERSE_GEOCODING_URI_TEMPLATE or " +
            "REVERSE_GEOCODING_QUERIES_PER_SECOND unconfigured, places will " +
            "not be labeled")
        return

    places = db.metadata.tables["places"]
    labdist = func.ST_Distance(places.c.coordinate, places.c.label_coordinate)
    t0 = time.time()
    for p in db.engine.execute(select(
            [   places.c.id,
                func.ST_AsGeoJSON(places.c.coordinate).label("geojson")],
            or_(labdist == None, labdist > DEST_RADIUS_MAX), # = clust dist / 2
            order_by=nullsfirst(desc(labdist)))):
        lon, lat = point_coordinates(p)
        url = url_template.format(lat=lat, lon=lon)
#        response = json.loads(urllib.request.urlopen(url, timeout=timeout).read())
        response = requests.get(url, timeout=timeout).json()
        names, nameslower = [], set()
        for prop in ["street", "name"]:
            for feat in response["features"]:
                name = feat["properties"].get(prop)
                name = name and re.split(",", name)[0]
                if name and name.lower() not in nameslower:
                    names.append(name)
                    nameslower.add(name.lower())

        label = " / ".join(names[:2])
        coordstr = "{:.4f}/{:.4f}".format(lat, lon)
        label = label or coordstr # fallback

        # Show progress due to rate limiting. Force encoding in case of pipe
        print(coordstr, label.encode("utf-8"))

        db.engine.execute(places.update(
            places.c.id == p.id,
            {"label": label, "label_coordinate": "POINT(%f %f)" % (lon, lat)}))

        # Enforce configured API queries per second rate limit
        time.sleep(1./qps)

        if time.time() - t0 >= timeout:
            break


def filter_device_data(maxtime=None):
    if not maxtime:
        maxtime = datetime.datetime.now()

    print("filter_device_data up to", maxtime)

    #TODO: Don't check for users that have been inactive for a long time.
    user_ids =  db.engine.execute(text("SELECT id FROM users;"))
    for id_row in user_ids:
        time = get_max_time_from_table("time", "device_data_filtered", "user_id", id_row["id"])
        device_data_rows = data_points_by_user_id_after(
            id_row["id"], time, maxtime)
        device_data_filterer = DeviceDataFilterer()
        device_data_filterer.generate_filtered_data(
            device_data_rows, id_row["id"])


def generate_distance_data():
    user_ids =  db.engine.execute(text("SELECT id FROM users;"))
    last_midnight = datetime.datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0)
    for id_row in user_ids:
        time = get_max_time_from_table("time", "travelled_distances", "user_id", id_row["id"]) + timedelta(days=1)
        update_user_distances(id_row["id"], time, last_midnight, False)

    # update rankings based on ratings
    query = text("""
        SELECT DISTINCT time FROM travelled_distances
        WHERE ranking IS NULL AND total_distance IS NOT NULL""")
    for row in db.engine.execute(query):
        generate_rankings(row[0])


def generate_global_statistics():
    query = """
        SELECT COALESCE(
            (SELECT max(time) + interval '1 day' FROM global_statistics),
            (SELECT min(time) FROM travelled_distances))"""
    time_start = db.engine.execute(text(query)).scalar()
    if time_start is None:
        return

    last_midnight = datetime.datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0)

    update_global_statistics(time_start, last_midnight)


def generate_trips():
    legs = db.metadata.tables["legs"]
    trips = db.metadata.tables["trips"]

    # Legs may become detached from users with multiple devices rewriting
    # history. Delete trips where associated legs have no user
    userless_od = select(
        [trips.c.id],
        legs.c.user_id.is_(None),
        trips.join(legs, legs.c.id.in_([trips.c.origin, trips.c.destination])))
    userless_intra = select(
        [legs.c.trip.distinct()],
        and_(legs.c.user_id.is_(None), legs.c.trip.isnot(None)))
    rowcount = db.engine.execute(trips.delete(or_(
        trips.c.id.in_(userless_od), trips.c.id.in_(userless_intra)))).rowcount
    if rowcount:
        print("Deleted %d trips with userless legs" % rowcount)

    # Find tripless user moves
    untripped = select([legs.c.user_id, legs.c.time_start]) \
        .where(and_(
            legs.c.user_id.isnot(None),
            legs.c.activity.isnot(None),
            legs.c.activity != "STILL",
            legs.c.trip.is_(None))) \
        .cte("untripped")

    # Find nearest origin and destination longstops containing tripless. Doing
    # this in one legs^3 join is ...slow... so do (legs^2)^2 instead
    orig, dest = [
        select([untripped.c.user_id,
                untripped.c.time_start.label("move_start"),
                aggregate(legs.c.time_start).label("time_start")]) \
            .select_from(untripped.join(legs, and_(
                beforeafter,
                legs.c.user_id == untripped.c.user_id,
                legs.c.activity == "STILL",
                legs.c.time_end - legs.c.time_start >= TRIP_STOP_DURATION))) \
            .group_by(untripped.c.user_id, untripped.c.time_start) \
            .alias(alias)
        for aggregate, beforeafter, alias
        in [(func.max, legs.c.time_start < untripped.c.time_start, "orig"),
            (func.min, legs.c.time_start > untripped.c.time_start, "dest")]]
    tripends = select([orig.c.user_id, orig.c.time_start, dest.c.time_start]) \
        .select_from(orig.join(dest, and_(
            orig.c.user_id == dest.c.user_id,
            orig.c.move_start == dest.c.move_start))) \
        .distinct() \
        .order_by(orig.c.user_id, orig.c.time_start)

    for user, ostart, dstart in db.engine.execute(tripends):
        with db.engine.begin() as t:
            # Tripless legs may appear in the middle of an existing trip when
            # user has multiple devices. Nuke trips that are in the way
            overlap_orig = select([trips.c.id]) \
                .select_from(trips.join(legs, and_(
                    legs.c.user_id == user,
                    legs.c.id == trips.c.origin,
                    legs.c.time_start >= ostart,
                    legs.c.time_start < dstart)))
            overlap_dest = select([trips.c.id]) \
                .select_from(trips.join(legs, and_(
                    legs.c.user_id == user,
                    legs.c.id == trips.c.destination,
                    legs.c.time_start > ostart,
                    legs.c.time_start <= dstart)))
            overlap_intra = select([legs.c.trip]) \
                .where(and_(
                    legs.c.user_id == user,
                    legs.c.time_start.between(ostart, dstart)))
            dels = db.engine.execute(trips.delete(or_(
                    trips.c.id.in_(overlap_orig),
                    trips.c.id.in_(overlap_dest),
                    trips.c.id.in_(overlap_intra))).returning(trips.c.id)) \
                .fetchall()
            if dels:
                print("Deleted overlap trips %s" % " ".join(
                    str(x[0]) for x in dels))

            # Find and associate trip legs
            sel = select([legs.c.id]) \
                .where(and_(
                    legs.c.user_id == user,
                    legs.c.time_start.between(ostart, dstart))) \
                .order_by(legs.c.time_start)
            triplegs = [x[0] for x in t.execute(sel).fetchall()]
            orig, intra, dest = triplegs[0], triplegs[1:-1], triplegs[-1]
            ins = trips.insert() \
                .values(origin=orig, destination=dest) \
                .returning(trips.c.id)
            trip = t.execute(ins).scalar()
            upd = legs.update().values(trip=trip).where(legs.c.id.in_(intra))
            t.execute(upd)
            print("u"+str(user), "t"+str(trip), "ostart", str(ostart)[:16], \
                "dstart", str(dstart)[:16], orig, intra, dest)


def mass_transit_cleanup():
    """Delete and vacuum mass transit live location data older than configured
    interval, for example
        MASS_TRANSIT_LIVE_KEEP_DAYS = 7"""

    # keep all data if nothing configured
    days = app.config.get("MASS_TRANSIT_LIVE_KEEP_DAYS")
    if not days:
        return

    # Snap deletion to daystart; be noisy as these can take quite a long time.
    # Also delete martians from the future, no use in preferring those forever.
    log.info("Deleting mass_transit_data older than %d days...", days)
    query = text("""
        DELETE FROM mass_transit_data
        WHERE time < date_trunc('day', now() - interval ':days days')
           OR time > date_trunc('day', now() + interval '2 days')""")
    delrows = db.engine.execute(query, days=days).rowcount
    log.info("Deleted %d rows of mass_transit_data.", delrows)
    if not delrows:
        return

    # vacuum to reuse space; cannot be wrapped in transaction, so another conn
    log.info("Vacuuming and analyzing mass_transit_data...")
    query = text("VACUUM ANALYZE mass_transit_data")
    conn = db.engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    conn.execute(query)
    conn.close()
    log.info("Vacuuming and analyzing mass_transit_data complete.")

    # Note, to free space rather than mark for reuse, e.g. after configuring a
    # lower retention limit, use VACUUM FULL. Not done automatically due to
    # locking, additional temporary space usage, and potential OOM on reindex.

# Retrieve vehicle positions from Siri real-time interface
# As of Aug-2018 HSL no longer uses this, other cities available
def retrieve_hsl_data():
    url = "http://api.digitransit.fi/realtime/vehicle-positions/v1/siriaccess/vm/json"
    # response = urllib.request.urlopen(url, timeout=50)
    # json_data = json.loads(response.read())
    response = requests.get(url, timeout=50)
    json_data = response.json()
    vehicle_data = json_data["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"][0]["VehicleActivity"]

    all_vehicles = []

    def vehicle_row(vehicle):
        timestamp = datetime.datetime.fromtimestamp(vehicle["RecordedAtTime"] / 1000) #datetime doesn't like millisecond accuracy
        line_name, line_type = interpret_jore(vehicle["MonitoredVehicleJourney"]["LineRef"]["value"])
        longitude = vehicle["MonitoredVehicleJourney"]["VehicleLocation"]["Longitude"]
        latitude = vehicle["MonitoredVehicleJourney"]["VehicleLocation"]["Latitude"]
        coordinate = 'POINT(%f %f)' % (longitude, latitude)
        vehicle_ref = vehicle["MonitoredVehicleJourney"]["VehicleRef"]["value"]

        return {
            'coordinate': coordinate,
            'line_name': line_name,
            'line_type': line_type,
            'time': timestamp,
            'vehicle_ref': vehicle_ref
        }

    for vehicle in vehicle_data:
        try:
            all_vehicles.append(vehicle_row(vehicle))
        except Exception:
            log.exception("Failed to handle vehicle record: %s" % vehicle)

    if all_vehicles:
        db.engine.execute(mass_transit_data_table.insert(all_vehicles))
    else:
        log.warning(
            "No mass transit data received at %s" % datetime.datetime.now())


# Process High-Frequency Positioning (HFP) MQTT callback
def handle_mass_transit(client, userdata, message):
    # print("%s %s" % (message.topic, message.payload))
    # b'{"VP":{"desi":"95","dir":"1","oper":22,"veh":888,"tst":"2018-08-12T19:15:01Z","tsi":1534101301,"spd":5.02,"hdg":277,"lat":60.219960,"long":25.099226,"acc":0.43,"dl":0,"odo":2053,"drst":0,"oday":"2018-08-12","jrn":463,"line":138,"start":"22:10"}}'
    payload = json.loads(message.payload.decode('utf-8'))['VP']
    longitude = payload['long']
    latitude = payload['lat']
    if (longitude != None) and (latitude != None):
        topics = message.topic.split('/')
        # print('topic', topic)
        # print(payload)
        vehicle_row = {'time': datetime.datetime.fromtimestamp(payload['tsi']),
                       'line_name': payload['desi'],
                       'line_type': topics[5].upper(),
                       'direction': int(payload['dir']),
                       'coordinate': 'POINT(%f %f)' % (longitude, latitude),
                       'vehicle_ref': str(payload['oper']) + '/' + str(payload['veh'])}
        db.engine.execute(mass_transit_data_table.insert([vehicle_row]))


def mass_transit_disconnect(client, userdata, rc):
    print("mass transit disconnect triggered")
    init_mass_transit_live_reception()


def init_mass_transit_live_reception():
    hostname = "mqtt.hsl.fi"
    port = 1883 # change to 443 for TLS
    keepalive = 36000 # seconds, default = 60
    global mass_transit_client
    mass_transit_client = mqtt.Client()
    mass_transit_client.on_message = handle_mass_transit
    mass_transit_client.on_disconnect = mass_transit_disconnect
    # ca_certs on Mac: "/etc/ssl/cert.pem"
    # ca_certs on Linux: "/etc/ssl/certs/ca-certificates.crt"
    # tls = { 'ca_certs': "/etc/ssl/cert.pem" }
    # mass_transit_client.tls_set(**tls)
    mass_transit_client.connect(hostname, port, keepalive)
    mass_transit_client.loop_start()
    mass_transit_client.subscribe("/hfp/v1/journey/ongoing/+/+/+/+/+/+/+/+/0/#")
    # subscribe.callback would have been simpler, but runs loop_forever by default
    # subscribe.callback(handle_mass_transit, "/hfp/v1/journey/ongoing/+/+/+/+/+/+/+/+/0/#", hostname="mqtt.hsl.fi", port=1883)


def get_max_time_from_table(time_column_name, table_name, id_field_name, id):
    query = '''
        SELECT MAX({0}) as time
        FROM {1}
        GROUP BY {2}
        HAVING {2} = :id;
    '''.format(time_column_name, table_name, id_field_name)
    time_row = db.engine.execute(text(query),
                                 id = id).fetchone()
    if time_row is None:
        time = datetime.datetime.strptime("1971-01-01", '%Y-%m-%d')
    else:
        time = time_row["time"]
    return time


def retrieve_transport_alerts():
    hsl_new = hsl_alert_request()
    if hsl_new:
        hsl_alerts_insert(hsl_new)
        for hsl_alert in hsl_new:
            for device_alert in match_pubtrans_alert(hsl_alert):  # match_pubtrans_alert_test(alert)  # For testing ptp_push - COMMENT OUT!
                push_ptp_alert(PTP_TYPE_PUBTRANS, device_alert)
    traffic_disorder_new = traffic_disorder_request()
    if traffic_disorder_new:
        traffic_disorder_insert(traffic_disorder_new)
        for disorder in traffic_disorder_new:
            if disorder["coordinate"] is not None:
                for device_alert in match_traffic_disorder(disorder):
                    push_ptp_alert(PTP_TYPE_DIGITRAFFIC, device_alert)


def retrieve_weather_info():
    # TODO: If either one returns an empty set, re-schedule fetch after a few minutes and try to keep some max_retry counter?
    weather_forecast_insert(fmi_forecast_request())
    weather_observations_insert(fmi_observations_request())


def delete_device_data_duplicates():
    rowcount = device_data_delete_duplicates()
    print('%d duplicate device_data points were deleted' % rowcount)


def set_device_data_waypoints():
    t = time.time()
    rowcount = device_data_waypoint_snapping()
    print("set_device_data_waypoints on %d points in %.2g seconds" % (
        rowcount, time.time() - t))


def set_leg_waypoints():
    t = time.time()

    dd = db.metadata.tables["device_data"]
    legs = db.metadata.tables["legs"]
    glue = db.metadata.tables["leg_waypoints"]

    legpoints = select(
        [legs.c.id, dd.c.waypoint_id, dd.c.time, dd.c.snapping_time],
        from_obj=dd.join(legs, and_(
            dd.c.device_id == legs.c.device_id,
            dd.c.time.between(legs.c.time_start, legs.c.time_end)))) \
        .alias("legpoints")
    done = select([glue.c.leg], distinct=True)
    nounsnapped = select(
        [legpoints.c.id],
        legpoints.c.id.notin_(done),
        group_by=legpoints.c.id,
        having=func.bool_and(legpoints.c.snapping_time.isnot(None)))
    newitems = select(
        [legpoints.c.id, legpoints.c.waypoint_id, func.min(legpoints.c.time)],
        legpoints.c.id.in_(nounsnapped),
        group_by=[legpoints.c.id, legpoints.c.waypoint_id]).alias("newitems")

    ins = glue.insert().from_select(["leg", "waypoint", "first"], newitems)
    rowcount = db.engine.execute(ins).rowcount
    print("set_leg_waypoints on %d rows in %.2g seconds" % (
        rowcount, time.time() - t))


def main_loop():
    while 1:
        time.sleep(1)
if __name__ == "__main__":
    initialize()
    try:
        main_loop()
    except KeyboardInterrupt:
        print('\nExiting by user request.\n', file=sys.stderr)
        mass_transit_client.loop_stop()
        sys.exit(0)


# test_disorder = {
#                 'record_creation_time': '2017-01-18 16:08:37.598+00',
#                 'disorder_id': 'GUID5000826501',
#                 'start_time': '2017-01-18 14:40:03.981+00',
#                 'end_time': '2017-01-18 16:08:37.598+00',
#                 'coordinate': '0101000020E61000002922C32ADED03840D8F50B76C31A4E40',
#                 'waypoint_id': 248252602092,
#                 'fi_description': '',
#                 'sv_description': '',
#                 'en_description': ''
#             }
#
# match_device_disorder("DISTINCT legs.id", disorder)
