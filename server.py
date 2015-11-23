#!/usr/bin/env python

import json
import hashlib
import re
import urllib2
import geoalchemy2 as ga2
import math
import svg_generation
from constants import *
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import date, timedelta
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
filtered_device_data_table = Table('device_data_filtered', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('device_id', Integer, ForeignKey('devices.id'), nullable=False),
                          Column('coordinate', ga2.Geography('point', 4326, spatial_index=False), nullable=False),
                          Column('time', TIMESTAMP, nullable=False),
                          Column('activity', activity_type_enum),
                          Column('waypoint_id', BigInteger),
                          Index('idx_device_data_filtered_time', 'time'),
                          Index('idx_device_data_filtered_device_id_time', 'device_id', 'time'))

filtered_device_data_table.create(bind=db.engine, checkfirst=True)

# travelled distances per day per device
travelled_distances_table = Table('travelled_distances', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('device_id', Integer, ForeignKey('devices.id'), nullable=False),
                          Column('time', Date, nullable=False),
                          Column('cycling', Float),
                          Column('walking', Float),
                          Column('running', Float),
                          Column('mass_transit_a', Float),
                          Column('mass_transit_b', Float),
                          Column('mass_transit_c', Float),
                          Column('car', Float),
                          Index('idx_travelled_distances_time', 'time'),
                          Index('idx_travelled_distances_device_id_time', 'device_id', 'time'))
travelled_distances_table.create(bind=db.engine, checkfirst=True)

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


metadata.create_all(bind=db.engine, checkfirst=True)

scheduler = BackgroundScheduler()

def initialize():
    scheduler.add_job(retrieve_hsl_data, "cron", second="*/28")
    #scheduler.add_job(filter_device_data, "cron", hour="3")
    #filter_device_data()
    scheduler.start()



# REST interface:

@app.route('/register', methods=['POST'])
def register_post():
    """
        Server should receive valid one-time tokan that can be used to authenticate user via Google+ API
        See: https://developers.google.com/+/web/signin/server-side-flow#step_6_send_the_authorization_code_to_the_server

    """
    data = request.json
    google_one_time_token = data['oneTimeToken']
    device_id = data['deviceId']
    installation_id = data['installationId']
    device_model = data['deviceModel']
    print 'deviceModel=' + str(device_model)

    # 1. authenticate with Google
    validation_data = authenticate_with_google_oauth(google_one_time_token)
    if validation_data is None:
        abort(403)  # auth failed

    account_google_id = validation_data['google_id']

    # The following hash value is also generated in client and used in authentication
    user_id = str(hashlib.sha256(str(account_google_id).encode('utf-8')).hexdigest()).upper()

    devices_table_id = None
    users_table_id = None

    # 2. Check if user has registered previously
    ext_users_table_id = get_users_table_id(user_id)
    if ext_users_table_id >= 0:
        users_table_id = ext_users_table_id

        # 3. Check if same device has registered with same user
        ext_user_id_for_device = get_users_table_id_for_device(device_id, installation_id)
        if ext_user_id_for_device >= 0:
            if ext_user_id_for_device != ext_users_table_id:
                # same device+installation is registered to other user
                print 'Re-registration attempt for different user'
                abort(403)

            print 'device re-registration detected -> using same device'
            devices_table_id = get_device_table_id(device_id, installation_id)

    # 4. create/update user to db
    if users_table_id < 0:
        stmt = users_table.insert({'google_refresh_token': str(validation_data['refresh_token']),
                                   'google_server_access_token': str(validation_data['access_token']),
                                   'user_id': str(user_id)})
        db.engine.execute(stmt)
        users_table_id = get_users_table_id(str(user_id))
    else:
        print 're-registration for same user detected -> using existing user account'
        stmt = users_table.update().values({'google_refresh_token': str(validation_data['refresh_token']),
                                            'google_server_access_token': str(
                                                validation_data['access_token'])}).where(text(
            'id={0}'.format(str(users_table_id))))

        db.engine.execute(stmt)



    # 5. Create/update device to db
    if devices_table_id is None:
        session_token = uuid4().hex
        device_insertion = devices_table.insert(
            {'user_id': users_table_id,
             'device_id': device_id,
             'installation_id': installation_id,
             'device_model': device_model,
             'token': session_token})
        db.engine.execute(device_insertion)
    else:
        update_last_activity(devices_table_id)
        session_token = get_session_token_for_device(devices_table_id)

    resp = jsonify({'sessionToken': session_token})
    return resp


@app.route('/authenticate', methods=['POST'])
def authenticate_post():
    json = request.json
    user_id = json['userId']
    device_id = json['deviceId']
    installation_id = json['installationId']

    # 1. check that user exists or abort
    verify_user_id(user_id)

    devices_table_id = get_device_table_id(device_id, installation_id)
    session_token = get_session_token_for_device(devices_table_id)
    if session_token is None:
        print 'User is not registered. userId=' + user_id
        abort(403)

    update_last_activity(devices_table_id)

    return jsonify({
        'sessionToken': session_token
    })


@app.route('/data', methods=['POST'])
def data_post():
    session_token = request.args['sessionToken']
    if session_token is None or session_token == '':
        abort(403)  # not authenticated user

    device_id = get_device_table_id_for_session(session_token)
    if device_id < 0:
        abort(403)  # not registered user

    data_points = request.json['dataPoints']

    # Remember, if a single point fails, the whole batch fails
    batch_size = 1024

    def batch_chunks(x):
        for i in xrange(0, len(x), batch_size):
            yield x[i:i + batch_size]

    def prepare_point(point):
        location = point['location']

        result = {
            'device_id': device_id,
            'coordinate': 'POINT(%f %f)' % (float(location['longitude']), float(location['latitude'])),
            'accuracy': float(location['accuracy']),
            'time': datetime.datetime.fromtimestamp(long(point['time']) / 1000.0)
        }
        result.update(prepare_point_activities(point))
        return result

    def prepare_point_activities(point):
        if 'activityData' not in point or 'activities' not in point['activityData']:
            return
        activities = point['activityData']['activities']

        def parse_activities():
            for activity in activities:
                activity_type = activity['activityType']
                if activity_type in activity_types:
                    yield {
                        'type': activity_type,
                        'confidence': int(activity['confidence'])
                    }

        sorted_activities = sorted(parse_activities(), key=lambda x: x['confidence'], reverse=True)
        result = {}

        if len(sorted_activities) > 0:
            result['activity_1'] = sorted_activities[0]['type']
            result['activity_1_conf'] = sorted_activities[0]['confidence']
            if len(sorted_activities) > 1:
                result['activity_2'] = sorted_activities[1]['type']
                result['activity_2_conf'] = sorted_activities[1]['confidence']
                if len(sorted_activities) > 2:
                    result['activity_3'] = sorted_activities[2]['type']
                    result['activity_3_conf'] = sorted_activities[2]['confidence']
                else:
                    result['activity_3'] = 'UNKNOWN'
                    result['activity_3_conf'] = 0
            else:
                result['activity_2'] = 'UNKNOWN'
                result['activity_2_conf'] = 0
                result['activity_3'] = 'UNKNOWN'
                result['activity_3_conf'] = 0
        return result

    for chunk in batch_chunks(data_points):
        batch = [prepare_point(x) for x in chunk]
        db.engine.execute(device_data_table.insert(batch))
    return jsonify({
    })


"""
* JSU: Devices are disabled since it's not secure to give out session tokens for all users
*
@app.route('/devices')
def devices():
    try:
        cols = table('devices', column('id'), column('token'))
        query = select([cols]).order_by(desc('token'))
        rows = db.engine.execute(query.compile())

        result = ""
        for row in rows:
            result += '%s = %s\n' % (row[1], row[0])
        return str(result)
    except Exception as e:
        print('Exception: ' + e.message)

    return ""
"""


@app.route('/device/<session_token>')
def device(session_token):
    try:
        device_id = get_device_table_id_for_session(session_token)

    except Exception as e:
        print 'Device-query - exception: ' + e.message
        device_id = -1

    if device_id >= 0:
        return jsonify({'sessionToken': session_token, 'deviceId': device_id})
    else:
        return jsonify({'error': "Invalid session token"})


@app.route('/maintenance/duplicates')
def maintenance_duplicates():
    with open('sql/delete_duplicate_device_data.sql', 'r') as sql_file:
        sql = sql_file.read()
        result = db.engine.execute(text(sql))
        return '%d duplicate data points were deleted' % (result.rowcount)


@app.route('/maintenance/snapping')
def maintenance_snapping():
    with open('sql/snapping.sql', 'r') as sql_file:
        sql = sql_file.read()
        result = db.engine.execute(text(sql))
        return 'Snapping was done to %d data points' % (result.rowcount)

massive_advanced_csv_query = """
      SELECT
        device_id, time,
        ST_Y(coordinate::geometry) as longitude,
        ST_X(coordinate::geometry) as latitude,
        accuracy,
        activity_1, activity_1_conf,
        activity_2, activity_2_conf,
        activity_3, activity_3_conf,
        waypoint_id
      FROM device_data
      ORDER BY time ASC
"""


@app.route('/csv/')
def export_csv():
    rows = db.engine.execution_options(
        stream_results=True).execute(text(massive_advanced_csv_query))
    return Response(generate_csv(rows), mimetype='text/csv')


@app.route('/csv/<page>')
def export_csv_block(page):
    entry_block_size = 10000

    if int(page) == 0:
        offset = int(page) * entry_block_size
    else:
        offset = (int(page) - 1) * entry_block_size
    limit = entry_block_size

    query = text(massive_advanced_csv_query + ' LIMIT :limit OFFSET :offset')
    rows = db.engine.execution_options(
        stream_results=True).execute(query, limit=limit, offset=offset)
    return Response(generate_csv(rows), mimetype='text/csv')


@app.route('/csv/waypoints')
def export_csv_waypoints():
    query = text(
        'SELECT id,\
            ST_Y(geo::geometry) as longitude,\
            ST_X(geo::geometry) as latitude\
            FROM waypoints')
    rows = db.engine.execute(query)
    return Response(generate_csv_waypoints(rows), mimetype='text/csv')


def generate_csv_waypoints(rows):
    yield '"wpt_id";"longitude";"latitude"\n'
    for row in rows:
        yield ';'.join(['"%s"' % (str(x)) for x in row]) + '\n'


@app.route('/visualize/<int:device_id>')
def visualize(device_id):
    return render_template('visualize.html',
                           api_key=app.config['MAPS_API_KEY'],
                           device_id=device_id)


@app.route('/visualize/<int:device_id>/geojson')
def visualize_device_geojson(device_id):
    if 'date' in request.args:
        date_start = datetime.datetime.strptime(request.args['date'], '%Y-%m-%d').date()
    else:
        date_start = date.today()

    date_end = date_start + timedelta(days=1)

    points = data_points_snapping(device_id, datetime.datetime.fromordinal(date_start.toordinal()),
                                  datetime.datetime.fromordinal(date_end.toordinal()))

    features = []
    waypoints = set()
    for point in points:
        activity_data = []
        if point['activity_1']:
            activity_data.append('{type:%s, conf:%d}' % (str(point['activity_1']), int(point['activity_1_conf'])))
        if point['activity_2']:
            activity_data.append('{type:%s, conf:%d}' % (str(point['activity_2']), int(point['activity_2_conf'])))
        if point['activity_3']:
            activity_data.append('{type:%s, conf:%d}' % (str(point['activity_3']), int(point['activity_3_conf'])))
        activity_info = 'activities: ' + ', '.join(activity_data)

        point_geo = json.loads(point['geojson'])
        features.append({
            'type': 'Feature',
            'geometry': point_geo,
            'properties': {
                'type': 'raw-point',
                'activity': str(point['activity_1']),
                'title': 'accuracy: %d\n%s' % (point['accuracy'], activity_info)
            }
        })
        if point['accuracy'] < 500:
            if point['waypoint_id']:
                waypoints.add(point['waypoint_id'])
    if len(waypoints) > 0:
        for row in db.engine.execute(text('''
            SELECT DISTINCT ON (id)
                ST_AsGeoJSON(geo), id
            FROM waypoints
            WHERE id = ANY (:waypoints ::bigint[])
        '''), waypoints=list(waypoints)):
            features.append({
                'type': 'Feature',
                'geometry': json.loads(row[0]),
                'properties': {
                    'type': 'route-point'
                }
            })
    geojson = {
        'type': 'FeatureCollection',
        'features': features
    }
    return jsonify(geojson)



@app.route("/grade_date/<requested_date>")
def grade_date(requested_date):
    date_start = requested_date
    date_end = str(datetime.datetime.strptime(requested_date, '%Y-%m-%d').date() + timedelta(days=1))
    return_string = ""
    device_id_rows = get_distinct_device_ids(date_start, date_end)
    for row in device_id_rows:
        device_id = str(row["device_id"])
        return_string += "DEVICE: " + device_id + "<br>"
        device_data_rows = data_points_snapping(device_id, date_start, date_end)
        return_string += rating_to_string(calculate_rating(device_data_rows))
        return_string += "<br><br><br>"

    if return_string == "":
        return_string = "No matches found!"

    return return_string

@app.route("/svg")
def svg():
    #end_time = datetime.datetime.now()
    end_time = datetime.datetime.strptime("2015-09-17", '%Y-%m-%d')
    start_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)
    end_time_string = end_time.strftime("%Y-%m-%d")
    start_time_string = start_time.strftime("%Y-%m-%d")
    TEMP_FIXED_DEVICE_ID = 80

    device_data_rows = data_points_snapping(TEMP_FIXED_DEVICE_ID, start_time_string, end_time_string)
    rating = calculate_rating(device_data_rows)
    return svg_generation.generate_energy_rating_svg(rating, start_time_string, end_time_string)





# Helper Functions:


class EnergyRating:
    def __init__(self, in_vehicle_distance, in_mass_transit_A_distance, in_mass_transit_B_distance, in_mass_transit_C_distance, on_bicycle_distance, running_distance, walking_distance):
        self.in_vehicle_distance = in_vehicle_distance
        self.on_bicycle_distance = on_bicycle_distance
        self.running_distance = running_distance
        self.walking_distance = walking_distance
        self.in_mass_transit_A_distance = in_mass_transit_A_distance
        self.in_mass_transit_B_distance = in_mass_transit_B_distance
        self.in_mass_transit_C_distance = in_mass_transit_C_distance
        self.total_distance = in_vehicle_distance + on_bicycle_distance + walking_distance + running_distance + in_mass_transit_A_distance + in_mass_transit_B_distance + in_mass_transit_C_distance
        if self.total_distance == 0:
            self.running_percentage = 0
            self.walking_percentage = 0
            self.in_vehicle_percentage = 0
            self.on_bicycle_percentage = 0
            self.in_mass_transit_A_percentage = 0
            self.in_mass_transit_B_percentage = 0
            self.in_mass_transit_C_percentage = 0
        else:
            self.running_percentage = running_distance / self.total_distance
            self.walking_percentage = walking_distance / self.total_distance
            self.in_vehicle_percentage = in_vehicle_distance / self.total_distance
            self.on_bicycle_percentage = on_bicycle_distance / self.total_distance
            self.in_mass_transit_A_percentage = in_mass_transit_A_distance / self.total_distance
            self.in_mass_transit_B_percentage = in_mass_transit_B_distance / self.total_distance
            self.in_mass_transit_C_percentage = in_mass_transit_C_distance / self.total_distance

        self.average_co2 = self.on_bicycle_percentage * ON_BICYCLE_CO2 + \
                           self.walking_percentage * WALKING_CO2 + \
                           self.running_percentage * RUNNING_CO2 + \
                           self.in_vehicle_percentage * IN_VEHICLE_CO2 + \
                           self.in_mass_transit_A_percentage * MASS_TRANSIT_A_CO2 + \
                           self.in_mass_transit_B_percentage * MASS_TRANSIT_B_CO2 + \
                           self.in_mass_transit_C_percentage * MASS_TRANSIT_C_CO2

        self.total_co2 = self.average_co2 * self.total_distance

        self.final_rating = (self.average_co2 - ON_BICYCLE_CO2) / IN_VEHICLE_CO2 # value becomes 0-1 with 1 being the worst.




'''
id integer NOT NULL,
device_id integer NOT NULL,
coordinate geography(Point,4326) NOT NULL,
accuracy double precision NOT NULL,
"time" timestamp without time zone NOT NULL,
activity_1 activity_type_enum,
activity_1_conf integer,
activity_2 activity_type_enum,
activity_2_conf integer,
activity_3 activity_type_enum,
activity_3_conf integer,
waypoint_id bigint,
snapping_time timestamp without time zone
'''

#activity_types = ('IN_VEHICLE', 'ON_BICYCLE', 'ON_FOOT', 'RUNNING', 'STILL', 'TILTING', 'UNKNOWN', 'WALKING')
#good_activities = ('IN_VEHICLE', 'ON_BICYCLE', 'RUNNING', 'WALKING')
def calculate_rating(device_data_rows):
    rows = device_data_rows.fetchall()
    bad_activities = ("UNKNOWN", "TILTING", "STILL")
    if len(rows) == 0:
        return EnergyRating(0, 0, 0, 0, 0, 0, 0)
    in_vehicle_distance = 0
    on_bicycle_distance = 0
    walking_distance = 0
    running_distance = 0
    previous_time = rows[0]["time"]
    current_activity = rows[0]["activity_1"]
    previous_location = json.loads(rows[0]["geojson"])["coordinates"]

    for row in rows[1:]:
        if row["activity_1"] in bad_activities and current_activity == "UNKNOWN":
            continue
        current_activity = row["activity_1"]
        current_time = row["time"]

        if (current_time - previous_time).total_seconds() > 300:
            previous_time = current_time
            continue

        current_location = json.loads(row["geojson"])["coordinates"]

        # from: http://stackoverflow.com/questions/1253499/simple-calculations-for-working-with-lat-lon-km-distance
        # The approximate conversions are:
        # Latitude: 1 deg = 110.574 km
        # Longitude: 1 deg = 111.320*cos(latitude) km

        xdiff = (previous_location[0] - current_location[0]) * 110.320 * math.cos((current_location[1] / 360) * math.pi)
        ydiff = (previous_location[1] - current_location[1]) * 110.574

        distance = (xdiff * xdiff + ydiff * ydiff)**0.5

        previous_location = current_location

        if current_activity == "IN_VEHICLE":
            in_vehicle_distance += distance
        elif current_activity == "ON_BICYCLE":
            on_bicycle_distance += distance
        elif current_activity == "RUNNING":
            running_distance += distance
        elif current_activity == "WALKING":
            walking_distance += distance

    return EnergyRating(in_vehicle_distance, 0, 0, 0, on_bicycle_distance, running_distance, walking_distance)


def filter_device_data():
    #date_end = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    #date_start = date_end - timedelta(days=1)

    end_time = datetime.datetime.strptime("2015-09-17", '%Y-%m-%d')
    start_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)
    end_time_string = end_time.strftime("%Y-%m-%d")
    start_time_string = start_time.strftime("%Y-%m-%d")

    device_id_rows = get_distinct_device_ids(start_time_string, end_time_string)

    DEVICE_ID_TEMP = 80
    device_data_rows = data_points_snapping(DEVICE_ID_TEMP, start_time_string, end_time_string)
    analyse_unfiltered_data(device_data_rows, DEVICE_ID_TEMP)

    #for row in device_id_rows:
    #    device_id = str(row["device_id"])
    #    device_data_rows = data_points_snapping(device_id, start_time_string, end_time_string)
    #    analyse_unfiltered_data(device_data_rows, device_id)


def reset_activity_weights(weights):
    for activity in weights:
        weights[activity] = 0

def get_best_activity(weights):
    max = 0
    best_activity = "NOT_SET" #default
    weights["WALKING"] += weights["ON_FOOT"]
    for activity in weights:
        if weights[activity] > max:
            max = weights[activity]
            best_activity = activity
    return best_activity

def flush_device_data_queue(device_data_queue, activity, device_id):
    if len(device_data_queue) == 0:
        return
    filtered_device_data = []
    for device_data_row in device_data_queue:
        current_location = json.loads(device_data_row["geojson"])["coordinates"]
        filtered_device_data.append({"activity" : activity,
                                     "device_id" : device_id,
                                     'coordinate': 'POINT(%f %f)' % (float(current_location[0]), float(current_location[1])),
                                     "time" : device_data_row["time"],
                                     "waypoint_id" : device_data_row["waypoint_id"]})
    db.engine.execute(filtered_device_data_table.insert(filtered_device_data))


def analyse_row_activities(row, current_weights):
    current_activity = "NOT_SET"
    if row["activity_3"] in current_weights:
        current_weights[row["activity_3"]] += 1
        current_activity = row["activity_3"]
    if row["activity_2"] in current_weights:
        current_weights[row["activity_2"]] += 2
        current_activity = row["activity_2"]
    if row["activity_1"] in current_weights:
        current_weights[row["activity_1"]] += 4
        current_activity = row["activity_1"]
    if current_activity == "ON_FOOT": #The dictionary performs this check in get_best_activity
        current_activity = "WALKING"
    return current_activity

def analyse_unfiltered_data(device_data_rows, device_id):
    print datetime.datetime.now()
    rows = device_data_rows.fetchall()
    if len(rows) == 0:
        return
    device_data_queue = []
    current_weights = {'IN_VEHICLE' : 0,
                       'ON_BICYCLE' : 0,
                       'RUNNING' : 0,
                       'WALKING' : 0,
                       'ON_FOOT' : 0}
    time_previous = rows[0]["time"]
    best_activity = "NOT_SET"
    previous_best_activity = "NOT_SET"
    consecutive_differences = 0

    for i in xrange(len(rows)):
        current_row = rows[i]
        if (current_row["time"] - time_previous).total_seconds() > MAX_POINT_TIME_DIFFERENCE:
            best_activity = get_best_activity(current_weights)
            if best_activity != "NOT_SET": #if false, no good activity was found
                flush_device_data_queue(device_data_queue, best_activity, device_id)
                previous_best_activity = best_activity
            reset_activity_weights(current_weights)
            device_data_queue = []


        time_previous = current_row["time"]
        current_activity = analyse_row_activities(current_row, current_weights)
        device_data_queue.append(current_row)

        best_activity = get_best_activity(current_weights)
        if best_activity != "NOT_SET" and current_activity != "NOT_SET" and current_activity != best_activity:
            consecutive_differences += 1
            if consecutive_differences >= CONSECUTIVE_DIFFERENCE_LIMIT:
                #Flush all but the last CONSECUTIVE_DIFFERENCE_LIMIT items
                flush_device_data_queue(device_data_queue[:-CONSECUTIVE_DIFFERENCE_LIMIT], best_activity, device_id)
                previous_best_activity = best_activity
                reset_activity_weights(current_weights)
                device_data_queue = device_data_queue[-CONSECUTIVE_DIFFERENCE_LIMIT:]
                consecutive_differences = 0
                #set the current weights to correspond with the remaining items
                for j in range(CONSECUTIVE_DIFFERENCE_LIMIT):
                    analyse_row_activities(rows[i-j], current_weights)
        else:
            consecutive_differences = 0

    if best_activity not in current_weights:
        best_activity = previous_best_activity
    if best_activity in current_weights:
        flush_device_data_queue(device_data_queue, best_activity, device_id)
    print datetime.datetime.now()


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

def rating_to_string(energy_rating):
    return_string = "\
    Distances:<br>\
    In vehicle: {in_vehicle_distance}<br>\
    On bicycle: {on_bicycle_distance}<br>\
    Running: {running_distance}<br>\
    Walking: {walking_distance}<br>\
    Total: {total_distance}<br>\
    <br>\
    Percentages:<br>\
    In vehicle: {in_vehicle_percentage}<br>\
    On bicycle: {on_bicycle_percentage}<br>\
    Running: {running_percentage}<br>\
    Walking: {walking_percentage}<br>\
    <br>\
    Average CO2 emission (g): {average_co2}<br>\
    Total CO2 emission (g): {total_co2}<br>".format(in_vehicle_distance=energy_rating.in_vehicle_distance,
                                    on_bicycle_distance=energy_rating.on_bicycle_distance,
                                    running_distance=energy_rating.running_distance,
                                    walking_distance=energy_rating.walking_distance,
                                    total_distance=energy_rating.total_distance,
                                    in_vehicle_percentage=energy_rating.in_vehicle_percentage,
                                    on_bicycle_percentage=energy_rating.on_bicycle_percentage,
                                    running_percentage=energy_rating.running_percentage,
                                    walking_percentage=energy_rating.walking_percentage,
                                    average_co2=energy_rating.average_co2,
                                    total_co2=energy_rating.total_co2)
    return return_string


jore_ferry_regex = re.compile("^1019")
jore_subway_regex = re.compile("^1300")
jore_rail_regex = re.compile("^300")
jore_tram_regex = re.compile("^10(0|10)")
jore_bus_regex = re.compile("^(1|2|4)...")

jore_tram_replace_regex = re.compile("^.0*")
jore_bus_replace_regex = re.compile("^.0*")

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


def generate_csv(rows):
    # Poor man's CSV generation. Doesn't handle escaping properly.
    # Python's CSV library doesn't handle unicode, and seems to be
    # tricky to wrap as a generator (expected by Flask)
    yield '"device_id";"time";"longitude";"latitude";"accuracy";"activity_guess_1";"activity_guess_1_conf";"activity_guess_2";"activity_guess_2_conf";"activity_guess_3";"activity_guess_3_conf";"waypoint_id"\n'

    def to_str(x):
        if x is None:
            return ''
        return str(x)

    for row in rows:
        yield ';'.join(['"%s"' % (to_str(x)) for x in row]) + '\n'


def get_distinct_device_ids(datetime_start, datetime_end):
    return db.engine.execute(text('''
        SELECT DISTINCT device_id
        FROM device_data
        WHERE time >= :date_start
        AND time < :date_end;
    '''), date_start=str(datetime_start), date_end=str(datetime_end))


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


def verify_user_id(user_id):
    if user_id is None or user_id == '':
        print 'empty user_id'
        abort(403)
    try:
        # TODO: Fix SQL injection
        query = select([table('users', column('id'))]).where(text("user_id='" + user_id + "'"))
        row = db.engine.execute(query).first()

        if not row:
            print 'auth: no return value'
            abort(403)
        return row[0]
    except DataError as e:
        print 'Exception: ' + e.message
        abort(403)


def verify_device_token(token):
    try:
        # TODO: Fix SQL injection
        query = select([table('devices', column('id'))]).where(text("token='" + token + "'"))
        row = db.engine.execute(query).first()

        if not row:
            abort(403)
        return row[0]
    except DataError as e:
        print 'Exception: ' + e.message
        abort(403)


def update_last_activity(devices_table_id):
    update = devices_table.update().values({'last_activity': datetime.datetime.now()}).where(text(
        "id=" + str(devices_table_id)))
    db.engine.execute(update)


def get_users_table_id_for_device(device_id, installation_id):
    try:
        # TODO: Fix SQL injection
        query = select([table('devices', column('user_id'))]).where(
            text("device_id='" + device_id + "' AND installation_id='" + installation_id + "'"))
        row = db.engine.execute(query).first()
        if not row:
            return -1
        return int(row[0])
    except DataError as e:
        print 'Exception: ' + e.message

    return -1


def get_device_table_id(device_id, installation_id):
    try:
        # TODO: Fix SQL injection
        query = select([table('devices', column('id'))]).where(
            text("device_id='" + device_id + "' AND installation_id='" + installation_id + "'"))
        row = db.engine.execute(query).first()
        if not row:
            return -1
        return int(row[0])
    except DataError as e:
        print 'Exception: ' + e.message

    return -1


def get_device_table_id_for_session(session_token):
    try:
        # TODO: Fix SQL injection
        query = select([table('devices', column('id'))]).where(text("token='" + session_token + "'"))
        row = db.engine.execute(query).first()
        if not row:
            return -1
        return int(row[0])
    except DataError:
        # invalid session token
        return -1


def get_users_table_id(user_id):
    """
    :param user_id: user_id (hash value)
    :return: users.id (PK, Integer)
    """
    try:
        # TODO: Fix SQL injection
        query = select([table('users', column('id'))]).where(text("user_id='" + user_id + "'"))
        row = db.engine.execute(query).first()
        if not row:
            return -1
        return int(row[0])
    except DataError as e:
        print 'Exception: ' + e.message

    return -1


def get_session_token_for_device(devices_table_id):
    if devices_table_id is None or devices_table_id < 0:
        return None
    try:
        # TODO: Fix SQL injection
        query = select([table('devices', column('token'))]).where(text("id='" + str(devices_table_id) + "'"))
        row = db.engine.execute(query).first()
        if not row:
            return None
        return row[0]
    except DataError as e:
        print 'Exception: ' + e.message

    return None


def authenticate_with_google_oauth(one_time_token):
    """
        doc: https://developers.google.com/api-client-library/python/guide/aaa_oauth
    :param one_time_token: one time token acquired from Google with mobile client
    :return: dictionary with data used in authorization
    """
    redirect_uri = app.config['AUTH_REDIRECT_URI']

    flow = flow_from_clientsecrets(CLIENT_SECRET_FILE,
                                   scope='profile',
                                   redirect_uri=redirect_uri)

    # step1 to acquire one_time_token is done in android client
    try:
        credentials = flow.step2_exchange(one_time_token)
    except FlowExchangeError as err:
        # invalid token

        print 'invalid token: ' + one_time_token + ". error: " + err.message
        return None

    return verify_and_get_account_id(credentials)


def verify_and_read_id_token(id_token, client_id):
    data = {}
    if id_token is not None:
        # Check that the ID Token is valid.
        try:
            # Client library can verify the ID token.
            jwt = verify_id_token(jsonify(id_token), client_id)

            data['valid_id_token'] = True
            data['google_id'] = jwt['sub']

        except AppIdentityError as error:
            print 'verify: AppIdentityError: ' + error.message
            data['valid_id_token'] = False
    else:
        print 'verify: credentials.id_token is None'

    return data


def verify_id_token_values(id_token):
    true_client_id = json.loads(open(CLIENT_SECRET_FILE, 'r').read())['web']['client_id']
    server_client_id = str(id_token['aud'])
    if server_client_id != true_client_id:
        print 'invalid server client id returned'
        abort(403)


def verify_and_get_account_id(credentials):
    """Verify an ID Token or an Access Token."""

    verify_id_token_values(credentials.id_token)

    data = {}
    data['google_id'] = str(credentials.id_token['sub'])
    data['access_token'] = credentials.access_token
    data['valid_access_token'] = True

    if credentials.refresh_token is not None:
        data['refresh_token'] = credentials.refresh_token

    http = httplib2.Http()
    credentials.authorize(http)

    return data

# App starting point:
if __name__ == '__main__':
    initialize()
    if app.debug:
        app.run(host='0.0.0.0', use_reloader=False)
    else:
        app.run()
