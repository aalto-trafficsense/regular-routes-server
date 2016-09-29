import datetime
from datetime import timedelta

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
from pyfiles.constants import *
from pyfiles.common_helpers import get_distance_between_coordinates
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

    # Find start of last good leg processed for each device.
    lastgoodstart = select(
        [legs.c.device_id, func.max(legs.c.time_start).label("time_start")],
        legs.c.activity != None,
        group_by=legs.c.device_id).alias("lastgoodstart")

    # Find activity for the last good legs.
    lastgood = select(
        [legs.c.device_id, legs.c.time_start, legs.c.activity],
        from_obj=lastgoodstart.join(legs, and_(
            lastgoodstart.c.device_id == legs.c.device_id,
            lastgoodstart.c.time_start == legs.c.time_start))).alias("lastgood")

    # Find end of processed legs, including terminator for each device.
    lastend = select(
        [legs.c.device_id, func.max(legs.c.time_end).label("lastend")],
        group_by=legs.c.device_id).alias("lastend")

    # If trailing new points, resume at start of last good leg, or first point.
    starts = select(
        [   devmax.c.device_id,
            func.coalesce(lastgood.c.time_start, devmax.c.firstpoint),
            lastgood.c.activity],
        or_(lastend.c.lastend == None, devmax.c.lastpoint > lastend.c.lastend),
        devmax \
            .outerjoin(lastgood, devmax.c.device_id == lastgood.c.device_id)\
            .outerjoin(lastend, devmax.c.device_id == lastend.c.device_id))

    # In repair mode, just start from the top.
    if repair:
        starts = select([devmax.c.device_id, devmax.c.firstpoint])

    for device, start, lastact in db.engine.execute(starts):
        # When the resumed leg is a stop, the stop condition is not necessarily
        # valid anymore from the refined entry point, making the stop
        # unresumable. Rewind by the minimum stop duration to make sure a
        # resumed stop is redetected.
        rewind = start
        if lastact == "STILL":
            rewind -= datetime.timedelta(seconds=DEST_DURATION_MIN)

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
        first = True
        for leg in filterer.generate_device_legs(points):
            print " ".join(["d"+str(device), str(leg["time_start"])[:19],
                str(leg["time_end"])[:19]] + [repr(leg.get(x)) for x in [
                "activity", "line_type", "line_name", "line_source"]]),

            # Ignore any legs in the rewind span.
            if leg["time_end"] < start:
                print "-> early"
                continue

            # Rewind can refine a start back inappropriately, don't let it.
            if leg["time_start"] < start:
                print "-> setstart",
                leg["time_start"] = start

            # Adjust leg for db entry
            leg.update({
                "device_id": device,
                "coordinate_start": func.ST_GeomFromGeoJSON(
                    leg["geojson_start"]),
                "coordinate_end": func.ST_GeomFromGeoJSON(
                    leg["geojson_end"])})
            del leg["geojson_start"]
            del leg["geojson_end"]

            # Don't touch if same leg already recorded.
            existing = db.engine.execute(legs.select(and_(
                legs.c.device_id == leg["device_id"],
                legs.c.time_start == leg["time_start"],
                legs.c.time_end == leg["time_end"]))).first()
            # oh don't want to compare id and stuff...
            if existing and all(leg.get(x) == existing[x] for x in [
                    "activity", "line_type", "line_name", "line_source"]):
                print "-> unchanged"
                continue

            # Replace overlapping legs.
            rowcount = db.engine.execute(legs.delete(and_(
                legs.c.device_id == leg["device_id"],
                legs.c.time_start <= leg["time_end"],
                legs.c.time_end >= leg["time_start"]))).rowcount
            if rowcount:
                print "-> delete %d" % rowcount,
            db.engine.execute(legs.insert(leg))
            print "-> insert"

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
                legs.c.time_start > usermax.c.lastend)),
        legs.join(devices, legs.c.device_id == devices.c.id) \
            .outerjoin(usermax, devices.c.user_id == usermax.c.user_id),
        group_by=[devices.c.user_id])

    # In repair mode, just start from the top.
    if repair:
        starts = select(
            [devices.c.user_id, func.min(legs.c.time_start)],
            legs.c.time_end < maxtime,
            legs.join(devices, legs.c.device_id == devices.c.id),
            group_by=[devices.c.user_id, legs.c.device_id])

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
            if lastend and lstart <= lastend:
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
        data_rows = get_filtered_device_data_points(id_row["id"], time, last_midnight)
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
    rows = filtered_data_rows.fetchall()
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