import datetime
from datetime import timedelta
from itertools import chain, count

import json
import os
import sys
import time
import re
import urllib2
from pyfiles.database_interface import (
    init_db, data_points_by_user_id_after, get_filtered_device_data_points,
    hsl_alerts_insert, weather_forecast_insert, weather_observations_insert,
    traffic_disorder_insert, match_pubtrans_alert, match_pubtrans_alert_test,
    match_traffic_disorder)
from pyfiles.push_messaging import push_ptp_alert  # push_ptp_pubtrans, push_ptp_traffic,
from pyfiles.push_messaging import PTP_TYPE_PUBTRANS, PTP_TYPE_DIGITRAFFIC
from pyfiles.device_data_filterer import DeviceDataFilterer
from pyfiles.energy_rating import EnergyRating
from pyfiles.common_helpers import (
    get_distance_between_coordinates, pairwise, trace_discard_sidesteps,
    interpret_jore)
from pyfiles.constants import *
from pyfiles.information_services import (
    hsl_alert_request, fmi_forecast_request, fmi_observations_request, traffic_disorder_request)
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from sqlalchemy.sql import and_, func, or_, select, text

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

env_var_value = os.getenv(SETTINGS_FILE_ENV_VAR, None)
if env_var_value is not None:
    print 'loading settings from: "' + str(env_var_value) + '"'
    app.config.from_envvar(SETTINGS_FILE_ENV_VAR)
else:
    print 'Environment variable "SETTINGS_FILE_ENV_VAR" was not defined -> using debug mode'
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
    print "initialising scheduler"
    scheduler = BackgroundScheduler()
    scheduler.start()
    scheduler.add_job(retrieve_hsl_data, "cron", second="*/30")
    run_daily_tasks()
    scheduler.add_job(run_hourly_tasks, "cron", minute=24)
    scheduler.add_job(run_daily_tasks, "cron", hour="3")
    scheduler.add_job(retrieve_transport_alerts, "cron", minute="*/10")
    scheduler.add_job(retrieve_weather_info, "cron", hour="6")
    print "scheduler init done"


def run_daily_tasks():
    # The order is important.
    generate_legs()
    filter_device_data()
    generate_distance_data()
    update_global_statistics()
    mass_transit_cleanup()


def run_hourly_tasks():
    generate_legs()


def generate_legs(maxtime=None, repair=False):
    """Record legs from stops and mobile activity found in device telemetry, up
    to now or given maxtime. In repair mode, re-evaluate and replace all
    changed legs."""

    if not maxtime:
        maxtime = datetime.datetime.now()

    print "generate_legs up to", maxtime

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
        and_(legs.c.activity != None),
        distinct=True).alias("rrlegs")

    # Find end of processed legs, including terminator for each device.
    lastleg = select(
        [legs.c.device_id, func.max(legs.c.time_end).label("time_end")],
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

        print "d"+str(device), "resume", str(start)[:19], \
            "rewind", str(rewind)[:19], str(len(points))+"p"

        filterer = DeviceDataFilterer() # not very objecty rly
        lastend = None
        newlegs = filterer.generate_device_legs(points, start)

        for (prevleg, prevmodes), (leg, legmodes) in pairwise(
                chain([(None, None)], newlegs)):

          with db.engine.begin() as t:

            lastend = leg["time_end"]

            print " ".join([
                "d"+str(device),
                str(leg["time_start"])[:19],
                str(leg["time_end"])[:19],
                leg["activity"]]),

            # Adjust leg for db entry
            gj0 = leg.pop("geojson_start", None)
            gj1 = leg.pop("geojson_end", None)
            leg.update({
                "device_id": device,
                "coordinate_start": gj0 and func.ST_GeomFromGeoJSON(gj0),
                "coordinate_end": gj1 and func.ST_GeomFromGeoJSON(gj1)})

            # Don't touch if same leg already recorded.
            existing = t.execute(legs.select(and_(*(
                legs.c[c] == leg[c] for c in leg.keys())))).first()
            if existing:
                print "-> unchanged",
                legid = existing.id
            else:
                # Replace legs overlapping on more than a transition point
                overlapstart = prevleg and prevleg["time_end"] or start
                rowcount = t.execute(legs.delete(and_(
                    legs.c.device_id == leg["device_id"],
                    legs.c.time_start < leg["time_end"],
                    legs.c.time_end > overlapstart))).rowcount
                if rowcount:
                    print "-> delete %d" % rowcount,
                ins = legs.insert(leg).returning(legs.c.id)
                legid = t.execute(ins).scalar()
                print "-> insert",

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
                    print "-> del", src, ex,
                    t.execute(modes.delete(and_(
                        modes.c.leg == legid, modes.c.source == src)))
                if nu is not None:
                    print "-> ins", src, nu,
                    t.execute(modes.insert().values(
                        leg=legid, source=src, mode=nu[0], line=nu[1]))

            print

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
            legs.c.user_id,
            legs.c.time_start,
            legs.c.time_end],
        and_(legs.c.activity != None, legs.c.time_end < maxtime),
        devices.join(legs, devices.c.id == legs.c.device_id))

    unattached = owned.where(legs.c.user_id == None).alias("unattached")
    attached = owned.where(legs.c.user_id != None).alias("attached")

    # Unattached legs without end of attached leg within, so eligible start.
    # The having clause is any aggregate over a column that is not null, so as
    # to find null only when no matches in the left join
    eligible = select(
        [unattached.c.owner, unattached.c.time_start],
        from_obj=unattached.outerjoin(attached, and_(
            unattached.c.owner == attached.c.owner,
            attached.c.time_end > unattached.c.time_start,
            attached.c.time_end <= unattached.c.time_end)),
        group_by=[unattached.c.owner, unattached.c.time_start],
        having=(func.min(attached.c.time_start) == None)).alias("eligible")

    # Find start of first eligible leg
    starts = select(
        [eligible.c.owner, func.min(eligible.c.time_start)],
        from_obj=eligible,
        group_by=[eligible.c.owner])

    # In repair mode, just start from the top.
    if repair:
        starts = select(
            [devices.c.user_id, func.min(legs.c.time_start)],
            legs.c.time_end < maxtime,
            legs.join(devices, legs.c.device_id == devices.c.id),
            group_by=[devices.c.user_id])

    for user, start in db.engine.execute(starts):
        # Ignore the special legacy user linking userless data
        if user == 0:
            continue

        print "u"+str(user), "start attach", start

        # Get unattached legs from user's devices in end time order, so shorter
        # legs get attached in favor of longer legs from a more idle device.
        unattached = select(
            [legs.c.id, legs.c.time_start, legs.c.time_end, legs.c.user_id],
            and_(
                devices.c.user_id == user,
                legs.c.time_start >= start,
                legs.c.time_end < maxtime,
                legs.c.activity != None),
            legs.join(devices, legs.c.device_id == devices.c.id),
            order_by=legs.c.time_end)

        lastend = None
        for lid, lstart, lend, luser in db.engine.execute(unattached):
            print " ".join(["u"+str(user), str(lstart)[:19], str(lend)[:19]]),
            if lastend and lstart < lastend:
                if luser is None:
                    print "-> detached"
                    continue
                db.engine.execute(legs.update(
                    legs.c.id==lid).values(user_id=None)) # detach
                print "-> detach"
                continue
            lastend = lend
            if luser == user:
                print "-> attached"
                continue
            db.engine.execute(legs.update(
                legs.c.id==lid).values(user_id=user)) # attach
            print "-> attach"

    # Cluster backlog in batches
    cluster_legs(1000)


def cluster_legs(limit):
    """New leg ends are clustered live by triggers; this can be used to cluster
    legs created before clustering."""

    print "cluster_legs up to", limit

    with db.engine.begin() as t:
        t.execute(text("SELECT legs_cluster(:limit)"), limit=limit)


def filter_device_data(maxtime=None):
    if not maxtime:
        maxtime = datetime.datetime.now()

    print "filter_device_data up to", maxtime

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
    ratings = []
    last_midnight = datetime.datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0)
    for id_row in user_ids:
        time = get_max_time_from_table("time", "travelled_distances", "user_id", id_row["id"]) + timedelta(days=1)
        data_rows = get_filtered_device_data_points(
            id_row["id"], time, last_midnight)

        # discard suspiciously sharp movement from bogus location jumps
        data_rows = trace_discard_sidesteps(data_rows, BAD_LOCATION_RADIUS)

        ratings += get_ratings_from_rows(data_rows, id_row["id"])
    if len(ratings) > 0:
        db.engine.execute(travelled_distances_table.insert(ratings))

    # update rankings based on ratings
    query = text("""
        SELECT DISTINCT time FROM travelled_distances
        WHERE ranking IS NULL AND total_distance IS NOT NULL""")
    for row in db.engine.execute(query):
        generate_rankings(row[0])


def get_ratings_from_rows(filtered_data_rows, user_id):
    ratings = []
    rows = list(filtered_data_rows)
    if len(rows) == 0:
        return ratings
    previous_time = rows[0]["time"]
    current_date = rows[0]["time"].replace(hour = 0, minute = 0, second = 0, microsecond = 0)
    previous_location = json.loads(rows[0]["geojson"])["coordinates"]
    rating = EnergyRating(user_id, date=current_date)
    for row in rows[1:]:
        current_activity = row["activity"]
        current_time = row["time"]
        current_location = json.loads(row["geojson"])["coordinates"]

        if (current_time - current_date).total_seconds() >= 60*60*24: #A full day
            current_date = current_time.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
            rating.calculate_rating()
            if not rating.is_empty():
                ratings.append(rating.get_data_dict())
            rating = EnergyRating(user_id, date=current_date)

        if (current_time - previous_time).total_seconds() > MAX_POINT_TIME_DIFFERENCE:
            previous_time = current_time
            previous_location = current_location
            continue

        distance = get_distance_between_coordinates(previous_location, current_location) / 1000.0

        previous_location = current_location

        if current_activity == "IN_VEHICLE":
            #TODO: handle FERRY somehow.
            if row["line_type"] == "TRAIN":
                rating.add_in_mass_transit_A_distance(distance)
            elif row["line_type"] in ("TRAM", "SUBWAY"):
                rating.add_in_mass_transit_B_distance(distance)
            elif row["line_type"] == "BUS":
                rating.add_in_mass_transit_C_distance(distance)
            else:
                rating.add_in_vehicle_distance(distance)
        elif current_activity == "ON_BICYCLE":
            rating.add_on_bicycle_distance(distance)
        elif current_activity == "RUNNING":
            rating.add_running_distance(distance)
        elif current_activity == "WALKING":
            rating.add_walking_distance(distance)

    rating.calculate_rating()
    if not rating.is_empty():
        ratings.append(rating.get_data_dict())
    return ratings


def update_global_statistics():
    query = """
        SELECT COALESCE(
            (SELECT max(time) + interval '1 day' FROM global_statistics),
            (SELECT min(time) FROM travelled_distances))"""
    time_start = db.engine.execute(text(query)).scalar()
    if time_start is None:
        return

    last_midnight = datetime.datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0)
    items = []
    while (time_start < last_midnight):
        time_end = time_start + timedelta(days=1)
        query = '''
            SELECT total_distance, average_co2
            FROM travelled_distances
            WHERE time >= :time_start
            AND time < :time_end
        '''
        travelled_distances_rows = db.engine.execute(text(query), time_start=time_start, time_end=time_end)
        items.append(get_global_statistics_for_day(travelled_distances_rows, time_start))
        time_start += timedelta(days=1)
    if items:
        db.engine.execute(global_statistics_table.insert(items))


def get_global_statistics_for_day(travelled_distances_rows, time):
    time_end = time + timedelta(days=1)
    time_start = time_end - timedelta(days=7)
    query = '''
        SELECT COUNT(DISTINCT user_id)
        FROM travelled_distances
        WHERE time < :time_end
        AND time >= :time_start
    '''
    user_id_count_row = db.engine.execute(text(query), time_start=time_start, time_end=time_end).fetchone()
    id_count = user_id_count_row["count"]

    distance_sum = 0
    total_co2 = 0
    for row in travelled_distances_rows:
        if row["total_distance"]:
            distance_sum += row["total_distance"]
            total_co2 += row["total_distance"] * row["average_co2"]
    if distance_sum == 0:
        average_co2 = 0
    else:
        average_co2 = total_co2 / distance_sum
    return {'time':time,
            'average_co2_usage':average_co2,
            'past_week_certificates_number':id_count,
            'total_distance':distance_sum}


def generate_rankings(time):
    time_end = time + timedelta(days=1)
    time_start = time_end - timedelta(days=7)
    query = '''
        SELECT user_id, total_distance, average_co2
        FROM travelled_distances
        WHERE time < :time_end
        AND time >= :time_start
    '''
    travelled_distances_rows = db.engine.execute(text(query), time_start=time_start, time_end=time_end)

    total_distances = {}
    total_co2 = {}
    totals = []
    for row in travelled_distances_rows:
        if row["total_distance"] is not None:
            if row["user_id"] in total_distances:
                total_distances[row["user_id"]] += row["total_distance"]
                total_co2[row["user_id"]] += row["average_co2"] * row["total_distance"]
            else:
                total_distances[row["user_id"]] = row["total_distance"]
                total_co2[row["user_id"]] = row["average_co2"] * row["total_distance"]
    for user_id in total_distances:
        totals.append((user_id, total_co2[user_id] / total_distances[user_id]))
    totals_sorted = sorted(totals, key=lambda average_co2: average_co2[1])
    batch = [
        {   "user_id": totals_sorted[i][0],
            "time": time,
            "ranking": i + 1 }
        for i in range(len(totals_sorted))]
    if batch:
        db.engine.execute(travelled_distances_table.insert(batch))


def mass_transit_cleanup():
    """Delete and vacuum mass transit live location data older than configured
    interval, for example
        MASS_TRANSIT_LIVE_KEEP_DAYS = 7"""

    # keep all data if nothing configured
    days = app.config.get("MASS_TRANSIT_LIVE_KEEP_DAYS")
    if not days:
        return

    # snap deletion to daystart; be noisy as these can take quite a long time
    log.info("Deleting mass_transit_data older than %d days...", days)
    query = text("""
        DELETE FROM mass_transit_data
        WHERE time < date_trunc('day', now() - interval ':days days')""")
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


def retrieve_hsl_data():
    url = "http://dev.hsl.fi/siriaccess/vm/json"
    response = urllib2.urlopen(url, timeout=50)
    json_data = json.loads(response.read())
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
        except Exception as e:
            log.exception("Failed to handle vehicle record: %s" % vehicle)

    if all_vehicles:
        db.engine.execute(mass_transit_data_table.insert(all_vehicles))
    else:
        log.warning(
            "No mass transit data received at %s" % datetime.datetime.now())


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


def main_loop():
    while 1:
        time.sleep(1)
if __name__ == "__main__":
    initialize()
    try:
        main_loop()
    except KeyboardInterrupt:
        print >> sys.stderr, '\nExiting by user request.\n'
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