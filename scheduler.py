
import json
import hashlib
import re
import urllib2
import geoalchemy2 as ga2
import math
import svg_generation
from pyfiles.device_data_filterer import DeviceDataFilterer
from pyfiles.energy_rating import EnergyRating
from pyfiles.constants import *
from datetime import date, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, abort, jsonify, request, render_template, Response
from flask.ext.sqlalchemy import SQLAlchemy
from oauth2client.client import *
from oauth2client.crypt import AppIdentityError
from sqlalchemy import MetaData, Table, Column, ForeignKey, Enum, BigInteger, Integer, String, Index, UniqueConstraint, Date, \
    Float
from sqlalchemy.dialects.postgres import DOUBLE_PRECISION, TIMESTAMP, UUID
from sqlalchemy.exc import DataError
from sqlalchemy.sql import text, func, column, table, select
from uuid import uuid4
import logging
logging.basicConfig()





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

db = SQLAlchemy(app)
metadata = MetaData()

'''
These types are the same as are defined in:
https://developer.android.com/reference/com/google/android/gms/location/DetectedActivity.html
'''
activity_types = ('IN_VEHICLE', 'ON_BICYCLE', 'ON_FOOT', 'RUNNING', 'STILL', 'TILTING', 'UNKNOWN', 'WALKING')
activity_type_enum = Enum(*activity_types, name='activity_type_enum')

'''
These types are the same as here:
https://github.com/HSLdevcom/navigator-proto/blob/master/src/routing.coffee#L43
'''
mass_transit_types = ("FERRY", "SUBWAY", "TRAIN", "TRAM", "BUS")
mass_transit_type_enum = Enum(*mass_transit_types, name='mass_transit_type_enum')



# Schema definitions:

# Table with one entry for each user
users_table = Table('users', metadata,
                    Column('id', Integer, primary_key=True),
                    Column('user_id', String, unique=True, nullable=False),  # hash value of Google Id
                    Column('google_refresh_token', String),
                    Column('google_server_access_token', String),
                    Column('register_timestamp', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                           server_default=func.current_timestamp()),
                    Index('idx_users_user_id', 'user_id'))

if not users_table.exists(bind=db.engine):
    users_table.create(bind=db.engine)
    """ create legacy user that can be used to link existing data that was added
        before user objects were added to some user.
    """
    db.engine.execute(users_table.insert({'id': 0, 'user_id': 'legacy-user'}))

# Table with one entry for each client device
devices_table = Table('devices', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('user_id', Integer, ForeignKey('users.id'), nullable=False, default=0),
                      Column('device_id', String, nullable=False),
                      Column('installation_id', UUID, nullable=False),
                      Column('device_model', String, default='(unknown)'),
                      Column('token', UUID, unique=True, nullable=False),
                      Column('created', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                             server_default=func.current_timestamp()),
                      Column('last_activity', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                             server_default=func.current_timestamp()),
                      UniqueConstraint('device_id', 'installation_id', name='uix_device_id_installation_id'),
                      Index('idx_devices_device_id_inst_id', 'device_id', 'installation_id'))

device_data_table = Table('device_data', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('device_id', Integer, ForeignKey('devices.id'), nullable=False),
                          Column('coordinate', ga2.Geography('point', 4326, spatial_index=False), nullable=False),
                          Column('accuracy', DOUBLE_PRECISION, nullable=False),
                          Column('time', TIMESTAMP, nullable=False),
                          Column('activity_1', activity_type_enum),
                          Column('activity_1_conf', Integer),
                          Column('activity_2', activity_type_enum),
                          Column('activity_2_conf', Integer),
                          Column('activity_3', activity_type_enum),
                          Column('activity_3_conf', Integer),
                          Column('waypoint_id', BigInteger),
                          Column('snapping_time', TIMESTAMP),
                          Index('idx_device_data_time', 'time'),
                          Index('idx_device_data_device_id_time', 'device_id', 'time'))

Index('idx_device_data_snapping_time_null', device_data_table.c.snapping_time, postgresql_where=device_data_table.c.snapping_time == None)

# device_data_table after filtering activities.
device_data_filtered_table = Table('device_data_filtered', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
                          Column('coordinate', ga2.Geography('point', 4326, spatial_index=False), nullable=False),
                          Column('time', TIMESTAMP, nullable=False),
                          Column('activity', activity_type_enum),
                          Column('waypoint_id', BigInteger),
                          Column('line_type', mass_transit_type_enum),
                          Column('line_name', String),
                          Index('idx_device_data_filtered_time', 'time'),
                          Index('idx_device_data_filtered_user_id_time', 'user_id', 'time'))

device_data_filtered_table.create(bind=db.engine, checkfirst=True)

# travelled distances per day per device
travelled_distances_table = Table('travelled_distances', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
                          Column('time', TIMESTAMP, nullable=False), #Only the date portion of time is used. TIMESTAMP datatype used for consistency.
                          Column('cycling', Float),
                          Column('walking', Float),
                          Column('running', Float),
                          Column('mass_transit_a', Float),
                          Column('mass_transit_b', Float),
                          Column('mass_transit_c', Float),
                          Column('car', Float),
                          Column('total_distance', Float),
                          Column('average_co2', Float),
                          Column('ranking', Integer),
                          UniqueConstraint('time', 'user_id', name="unique_user_id_and_time_on_travelled_distances"),
                          Index('idx_travelled_distances_time', 'time'),
                          Index('idx_travelled_distances_device_id_time', 'user_id', 'time'))
if not travelled_distances_table.exists(bind=db.engine):
    travelled_distances_table.create(bind=db.engine, checkfirst=True)
    db.engine.execute(text('''
        CREATE RULE "travelled_distances_table_duplicate_update" AS ON INSERT TO "travelled_distances"
        WHERE EXISTS(SELECT 1 FROM travelled_distances
                    WHERE (user_id, time)=(NEW.user_id, NEW.time))
        DO INSTEAD UPDATE travelled_distances
            SET ranking = NEW.ranking
            WHERE user_id = NEW.user_id
            AND time = NEW.time;
        '''))


# HSL mass transit vehicle locations.
mass_transit_data_table = Table('mass_transit_data', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('coordinate', ga2.Geography('point', 4326, spatial_index=True), nullable=False),
                          Column('time', TIMESTAMP, nullable=False),
                          Column('line_type', mass_transit_type_enum, nullable=False),
                          Column('line_name', String, nullable=False),
                          Column('vehicle_ref', String, nullable=False),
                          UniqueConstraint('time', 'vehicle_ref', name="unique_vehicle_and_timestamp"),
                          Index('idx_mass_transit_data_time', 'time'),
                          Index('idx_mass_transit_data_vehicle_ref_time', 'vehicle_ref', 'time'))


if not mass_transit_data_table.exists(bind=db.engine):
    mass_transit_data_table.create(bind=db.engine, checkfirst=True)
    db.engine.execute(text('''
        CREATE RULE "mass_transit_data_table_duplicate_ignore" AS ON INSERT TO "mass_transit_data"
        WHERE EXISTS(SELECT 1 FROM mass_transit_data
                    WHERE (vehicle_ref, time)=(NEW.vehicle_ref, NEW.time))
        DO INSTEAD NOTHING;
        '''))

global_statistics_table = Table('global_statistics', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('time', TIMESTAMP, nullable=False), #Only the date portion of time is used. TIMESTAMP datatype used for consistency.
                          Column('average_co2_usage', Float), #Daily co2 usage
                          Column('past_week_certificates_number', Integer, nullable=False),
                          Column('total_distance', Float, nullable=False), #Daily amount of distance
                          Index('idx_global_statistics_time', 'time'),)
global_statistics_table.create(bind=db.engine, checkfirst=True)


metadata.create_all(bind=db.engine, checkfirst=True)







def initialize():
    scheduler = BackgroundScheduler()
    scheduler.add_job(retrieve_hsl_data, "cron", second="*/28")
    scheduler.add_job(run_daily_tasks, "cron", hour="3")
    run_daily_tasks()
    print "scheduler init done"
    scheduler.start()

def run_daily_tasks():
    # The order is important.
    filter_device_data()
    generate_distance_data()
    update_global_statistics()

def filter_device_data():
    #TODO: Don't check for users that have been inactive for a long time.
    user_ids =  db.engine.execute(text("SELECT id FROM users;"))
    for id_row in user_ids:
        time = get_max_time_from_table("time", "device_data_filtered", "user_id", id_row["id"])
        device_data_rows = data_points_by_user_id(id_row["id"], time, datetime.datetime.now())
        device_data_filterer = DeviceDataFilterer(db, device_data_filtered_table)
        device_data_filterer.analyse_unfiltered_data(device_data_rows, id_row["id"])



def generate_distance_data():
    user_ids =  db.engine.execute(text("SELECT id FROM users;"))
    ratings = []
    for id_row in user_ids:
        time = get_max_time_from_table("time", "travelled_distances", "user_id", id_row["id"]) + timedelta(days=1)
        last_midnight = datetime.datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)
        data_rows = get_filtered_device_data_points(id_row["id"], time, last_midnight)
        ratings += get_ratings_from_rows(data_rows, id_row["id"])
    if len(ratings) > 0:
        db.engine.execute(travelled_distances_table.insert(ratings))

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


        # from: http://stackoverflow.com/questions/1253499/simple-calculations-for-working-with-lat-lon-km-distance
        # The approximate conversions are:
        # Latitude: 1 deg = 110.574 km
        # Longitude: 1 deg = 111.320*cos(latitude) km

        x_diff = (previous_location[0] - current_location[0]) * 110.320 * math.cos((current_location[1] / 360) * math.pi)
        y_diff = (previous_location[1] - current_location[1]) * 110.574

        distance = (x_diff * x_diff + y_diff * y_diff)**0.5

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
    query = '''
        SELECT MAX(time) as time
        FROM global_statistics
    '''
    time_row = db.engine.execute(text(query)).fetchone()
    if time_row["time"] is None:
        query = '''
            SELECT MIN(time) as time
            FROM travelled_distances
        '''
        time_row = db.engine.execute(text(query)).fetchone()
        if time_row["time"] is None:
            return
    time_start = time_row["time"].replace(hour=0,minute=0,second=0,microsecond=0)
    last_midnight = datetime.datetime.now().replace(hour=0,minute=0,second=0,microsecond=0)
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
        generate_rankings(time_start)
        time_start += timedelta(days=1)
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
    update_query = ""
    for i in range(len(totals_sorted)):
        update_query += '''
            INSERT INTO travelled_distances
            (user_id, time, ranking)
            VALUES
            ({0}, :time, {1});
        '''.format(totals_sorted[i][0], i + 1)
    if update_query != "":
        db.engine.execute(text(update_query), time=time)



def retrieve_hsl_data():
    url = "http://dev.hsl.fi/siriaccess/vm/json"
    response = urllib2.urlopen(url)
    json_data = json.loads(response.read())
    vehicle_data = json_data["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"][0]["VehicleActivity"]

    all_vehicles = []

    for vehicle in vehicle_data:
        timestamp = datetime.datetime.fromtimestamp(vehicle["RecordedAtTime"] / 1000) #datetime doesn't like millisecond accuracy
        line_name, line_type = interpret_jore(vehicle["MonitoredVehicleJourney"]["LineRef"]["value"])
        longitude = vehicle["MonitoredVehicleJourney"]["VehicleLocation"]["Longitude"]
        latitude = vehicle["MonitoredVehicleJourney"]["VehicleLocation"]["Latitude"]
        coordinate = 'POINT(%f %f)' % (longitude, latitude)
        vehicle_ref = vehicle["MonitoredVehicleJourney"]["VehicleRef"]["value"]

        vehicle_item = {
            'coordinate': coordinate,
            'line_name': line_name,
            'line_type': line_type,
            'time': timestamp,
            'vehicle_ref': vehicle_ref
        }
        all_vehicles.append(vehicle_item)

    db.engine.execute(mass_transit_data_table.insert(all_vehicles))


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


def get_distinct_device_ids(datetime_start, datetime_end):
    return db.engine.execute(text('''
        SELECT DISTINCT device_id
        FROM device_data
        WHERE time >= :date_start
        AND time < :date_end;
    '''), date_start=str(datetime_start), date_end=str(datetime_end))


def get_filtered_device_data_points(user_id, datetime_start, datetime_end):
    query = '''
        SELECT time,
            ST_AsGeoJSON(coordinate) AS geojson,
            activity
        FROM device_data_filtered
        WHERE user_id = :user_id
        AND time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''
    points =  db.engine.execute(text(query), user_id=user_id, time_start=datetime_start, time_end=datetime_end)
    return points

def data_points_by_user_id(user_id, datetime_start, datetime_end):
    query = '''
        SELECT device_id,
            ST_AsGeoJSON(coordinate) AS geojson,
            activity_1, activity_1_conf,
            activity_2, activity_2_conf,
            activity_3, activity_3_conf,
            waypoint_id,
            time
        FROM device_data
        WHERE device_id IN (SELECT id FROM devices
                                 WHERE user_id = :user_id)
        AND time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''
    points =  db.engine.execute(text(query), user_id=user_id, time_start=datetime_start, time_end=datetime_end)
    return points

def data_points_snapping(device_id, datetime_start, datetime_end):
    qstart = '''
        SELECT id,
            ST_AsGeoJSON(coordinate) AS geojson,
            accuracy,
            activity_1, activity_1_conf,
            activity_2, activity_2_conf,
            activity_3, activity_3_conf,
            waypoint_id,
            time
        FROM device_data
    '''
    if device_id == 0:
        qstring = qstart + '''
        WHERE time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''
        points = db.engine.execute(text(qstring), time_start=datetime_start, time_end=datetime_end)
    else:
        qstring = qstart + '''
        WHERE device_id = :device_id
        AND time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''
        points =  db.engine.execute(text(qstring), device_id=device_id, time_start=datetime_start, time_end=datetime_end)
    return points

def data_points_filtered(user_id, datetime_start, datetime_end):
    qstring = '''
        SELECT id,
            ST_AsGeoJSON(coordinate) AS geojson,
            activity,
            time
        FROM device_data_filtered
        WHERE user_id = :user_id
        AND time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''
    points =  db.engine.execute(text(qstring), user_id=user_id, time_start=datetime_start, time_end=datetime_end)
    return points

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