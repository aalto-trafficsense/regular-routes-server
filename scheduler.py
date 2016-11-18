import datetime
from datetime import timedelta
from itertools import chain

import json
import os
import sys
import time
import re
import urllib2
from pyfiles.database_interface import (
    init_db, data_points_by_user_id_after, get_filtered_device_data_points)
from pyfiles.device_data_filterer import DeviceDataFilterer
from pyfiles.energy_rating import EnergyRating
from pyfiles.common_helpers import (
    get_distance_between_coordinates, pairwise, trace_discard_sidesteps)
from pyfiles.constants import *
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
    scheduler.add_job(generate_legs, "cron", minute=24)
    scheduler.add_job(run_daily_tasks, "cron", hour="3")
    print "scheduler init done"


def run_daily_tasks():
    # The order is important.
    generate_legs()
    filter_device_data()
    generate_distance_data()
    update_global_statistics()
    mass_transit_cleanup()


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
            devmax.c.device_id, devmax.c.firstpoint, devmax.c.firstpoint])

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

        for prevleg, leg in pairwise(chain([None], newlegs)):
            lastend = leg["time_end"]

            print " ".join(["d"+str(device), str(leg["time_start"])[:19],
                str(leg["time_end"])[:19]] + [repr(leg.get(x)) for x in [
                "activity", "line_type", "line_name", "line_source"]]),

            # Adjust leg for db entry
            gj0 = leg.pop("geojson_start", None)
            gj1 = leg.pop("geojson_end", None)
            leg.update({
                "device_id": device,
                "coordinate_start": gj0 and func.ST_GeomFromGeoJSON(gj0),
                "coordinate_end": gj1 and func.ST_GeomFromGeoJSON(gj1)})

            # Don't touch if same leg already recorded. Ignore id, user_id,
            # cluster_start/end, handled elsewhere.
            existing = db.engine.execute(legs.select(and_(
                legs.c.device_id == leg["device_id"],
                legs.c.time_start == leg["time_start"],
                legs.c.time_end == leg["time_end"],
                legs.c.coordinate_start == leg["coordinate_start"],
                legs.c.coordinate_end == leg["coordinate_end"],
                legs.c.activity == leg["activity"],
                legs.c.line_type == leg.get("line_type"),
                legs.c.line_name == leg.get("line_name"),
                legs.c.line_source == leg.get("line_source")))).first()

            if existing:
                print "-> unchanged"
                continue

            # Replace legs overlapping on more than a transition point
            overlapstart = prevleg and prevleg["time_end"] or start
            rowcount = db.engine.execute(legs.delete(and_(
                legs.c.device_id == leg["device_id"],
                legs.c.time_start < leg["time_end"],
                legs.c.time_end > overlapstart))).rowcount
            if rowcount:
                print "-> delete %d" % rowcount,
            db.engine.execute(legs.insert(leg))
            print "-> insert"

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

    # Find end of last leg attached to each user.
    usermax = select(
        [legs.c.user_id, func.max(legs.c.time_end).label("lastend")],
        legs.c.user_id != None,
        group_by=legs.c.user_id).alias("usermax")

    # Find later start of unattached legs for user's devices.
    starts = select(
        [devices.c.user_id, func.min(legs.c.time_start)],
        and_(
            legs.c.user_id == None,
            legs.c.activity != None,
            legs.c.time_end < maxtime,
            or_(usermax.c.lastend == None,
                legs.c.time_start >= usermax.c.lastend)),
        legs.join(devices, legs.c.device_id == devices.c.id) \
            .outerjoin(usermax, devices.c.user_id == usermax.c.user_id),
        group_by=[devices.c.user_id])

    # In repair mode, just start from the top.
    if repair:
        starts = select(
            [devices.c.user_id, func.min(legs.c.time_start)],
            legs.c.time_end < maxtime,
            legs.join(devices, legs.c.device_id == devices.c.id),
            group_by=[devices.c.user_id])

    for user, start in db.engine.execute(starts):
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


def interpret_jore(jore_code):
    if re.search(jore_ferry_regex, jore_code):
        line_name = "Ferry"
        line_type = "FERRY"
    elif re.search(jore_subway_regex, jore_code):
        line_name = jore_code[4:5]
        line_type = "SUBWAY"
    elif re.search(jore_rail_regex, jore_code):
        line_name = jore_code[4:5]
        line_type = "TRAIN"
    elif re.search(jore_tram_regex, jore_code):
        line_name = re.sub(jore_tram_replace_regex, "", jore_code)
        line_type = "TRAM"
    elif re.search(jore_bus_regex, jore_code):
        line_name = re.sub(jore_tram_replace_regex, "", jore_code)
        line_type = "BUS"
    else:
        # unknown, assume bus
        line_name = jore_code
        line_type = "BUS"
    return line_name, line_type



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