#!/usr/bin/env python

import json
import random
import string
import hashlib
import re
import urllib2
import geoalchemy2 as ga2
import math
import svg_generation
from device_data_filterer import DeviceDataFilterer
from energy_rating import EnergyRating
from constants import *
from datetime import date, timedelta
from flask import Flask, abort, jsonify, request, render_template, Response
from flask import make_response, session, send_file
from flask.ext.sqlalchemy import SQLAlchemy
from oauth2client.client import *
from oauth2client.crypt import AppIdentityError
from sqlalchemy import MetaData, Table, Column, ForeignKey, Enum, BigInteger, Integer, String, Index, UniqueConstraint, Date, \
    Float
from sqlalchemy.dialects.postgres import DOUBLE_PRECISION, TIMESTAMP, UUID
from sqlalchemy.exc import DataError
from sqlalchemy.sql import text, func, column, table, select
from uuid import uuid4

# from simplekv.memory import DictStore
from simplekv.db.sql import SQLAlchemyStore
from flask_kvsession import KVSessionExtension

import logging
logging.basicConfig()

APPLICATION_NAME = 'TrafficSense'

SETTINGS_FILE_ENV_VAR = 'REGULARROUTES_SETTINGS'
CLIENT_SECRET_FILE_NAME = 'client_secrets.json'

# set settings dir from env.var for settings file. fallback dir is server.py file's parent dir
settings_dir_path = os.path.abspath(os.path.dirname(os.getenv(SETTINGS_FILE_ENV_VAR, os.path.abspath(__file__))))
CLIENT_SECRET_FILE = os.path.join(settings_dir_path, CLIENT_SECRET_FILE_NAME)
CLIENT_ID = json.loads(open(CLIENT_SECRET_FILE, 'r').read())['web']['client_id']


app = Flask(__name__)

# Memory-resident session storage, see the simplekv documentation for details
# store = DictStore()

# This will replace the app's session handling
# KVSessionExtension(store, app)

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

# Run session storage also on SQLAlchemy
store = SQLAlchemyStore(db.engine, metadata, 'kv_session')
KVSessionExtension(store, app)

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
        date_start = datetime.datetime.strptime(request.args['date'], '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        date_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    date_end = date_start + timedelta(hours=24)

    points = data_points_snapping(device_id, date_start, date_end)

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

# Browser sign-in procedures

@app.route('/', methods=['GET'])
def index():
  """Initialize a session for the current user, and render index.html."""
  # Create a state token to prevent request forgery.
  # Store it in the session for later validation.
  state = ''.join(random.choice(string.ascii_uppercase + string.digits)
                  for x in xrange(32))
  session['state'] = state
  # Set the Client ID, Token State, and Application Name in the HTML while
  # serving it.
  response = make_response(
      render_template('index.html',
                      CLIENT_ID=CLIENT_ID,
                      STATE=state,
                      RR_URL_PREFIX=app.config['RR_URL_PREFIX'],
                      APPLICATION_NAME=APPLICATION_NAME))
  response.headers['Content-Type'] = 'text/html'
  return response

@app.route('/connect', methods=['POST'])
def connect():
  """Exchange the one-time authorization code for a token and
  store the token in the session."""
  # Ensure that the request is not a forgery and that the user sending
  # this connect request is the expected user.
  print 'Session state returns: ' + session.get('state')
  if request.args.get('state', '') != session.get('state'):
    response = make_response(json.dumps('Invalid state parameter.'), 401)
    response.headers['Content-Type'] = 'application/json'
    print '401 due to invalid state parameter.'
    return response
  # Delete the one-time token - page refresh required to re-connect
  del session['state']

  code = request.data

  try:
    # Upgrade the authorization code into a credentials object
    oauth_flow = flow_from_clientsecrets(CLIENT_SECRET_FILE,
                                         scope='profile',
                                         redirect_uri='postmessage')
    credentials = oauth_flow.step2_exchange(code)
  except FlowExchangeError as err:
    # invalid token
    print 'Invalid token: ' + code + ". error: " + err.message
    response = make_response(
        json.dumps('Failed to upgrade the authorization code.'), 401)
    response.headers['Content-Type'] = 'application/json'
    return response

  # An ID Token is a cryptographically-signed JSON object encoded in base 64.
  # Normally, it is critical that you validate an ID Token before you use it,
  # but since you are communicating directly with Google over an
  # intermediary-free HTTPS channel and using your Client Secret to
  # authenticate yourself to Google, you can be confident that the token you
  # receive really comes from Google and is valid. If your server passes the
  # ID Token to other components of your app, it is extremely important that
  # the other components validate the token before using it.
  google_id = verify_and_get_account_id(credentials)['google_id']

  stored_credentials = session.get('credentials')
  stored_google_id = session.get('google_id')
  if stored_credentials is not None and google_id == stored_google_id:
    response = make_response(json.dumps('Current user is already connected.'),
                             200)
    response.headers['Content-Type'] = 'application/json'
    return response
  # Store the access token in the session for later use.
  session['credentials'] = credentials
  session['google_id'] = google_id
  # Find and store the RegularRoutes user id
  user_hash_id = str(hashlib.sha256(str(google_id).encode('utf-8')).hexdigest()).upper()
  user_id = get_users_table_id(user_hash_id)
  if user_id < 0:
      # No data for the user -> show the nodata -page
      print 'No data found for the current user.'
      response = make_response(json.dumps('Nodata.'), 200)
      response.headers['Content-Type'] = 'application/json'
      return response
  session['rr_user_id'] = user_id
  response = make_response(json.dumps('Successfully connected user.'), 200)
  response.headers['Content-Type'] = 'application/json'
  return response


@app.route('/disconnect', methods=['POST'])
def disconnect():
  """Revoke current user's token and reset their session."""

  # Only disconnect a connected user.
  credentials = session.get('credentials')
  if credentials is None:
    response = make_response(json.dumps('Current user not connected.'), 401)
    response.headers['Content-Type'] = 'application/json'
    return response

  # Execute HTTP GET request to revoke current token.
  access_token = credentials.access_token
  url = 'https://accounts.google.com/o/oauth2/revoke?token=%s' % access_token
  h = httplib2.Http()
  result = h.request(url, 'GET')[0]

  if result['status'] == '200':
    # Reset the user's session.
    del session['credentials']
    if session.get('rr_user_id') != None: del session['rr_user_id']
    response = make_response(json.dumps('Successfully disconnected.'), 200)
    response.headers['Content-Type'] = 'application/json'
    return response
  else:
    # For whatever reason, the given token was invalid.
    response = make_response(
        json.dumps('Failed to revoke token for given user.', 400))
    response.headers['Content-Type'] = 'application/json'
    return response

@app.route('/signedout')
def signed_out():
    """User disconnected from the service."""
    return render_template('signedout.html')

@app.route('/menu')
def regularroutes_menu():
    """User disconnected from the service."""
    user_id = session.get('rr_user_id')
    if user_id == None:
        # Not authenticated -> throw back to front page
        return index()
    return render_template('menu.html')

@app.route('/nodata')
def no_data():
    """No data was found for this user account."""
    return render_template('nodata.html')

@app.route('/energymap')
def energy():
    """Draw the energy consumption map of the user."""
    user_id = session.get('rr_user_id')
    if user_id == None:
        # Not authenticated -> throw back to front page
        return index()
    return render_template('energy.html',
                           api_key=app.config['MAPS_API_KEY'])


@app.route('/energy/geojson')
def energy_device_geojson():
    if 'date' in request.args:
        date_start = datetime.datetime.strptime(request.args['date'], '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        date_start = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # date_start = datetime.datetime.strptime("2015-11-11", '%Y-%m-%d')
    date_end = date_start + timedelta(hours=24)

    user_id = session.get('rr_user_id')
    if user_id == None:
        response = make_response(json.dumps('No user data in current session.'), 401)
        response.headers['Content-Type'] = 'application/json'
        return response

    points = data_points_filtered(user_id, datetime.datetime.fromordinal(date_start.toordinal()),
                                  datetime.datetime.fromordinal(date_end.toordinal()))
    # points = data_points_filtered(user_id, date_start, date_end)

    features = []
    for point in points:
        point_geo = json.loads(point['geojson'])
        features.append({
            'type': 'Feature',
            'geometry': point_geo,
            'properties': {
                'type': 'raw-point',
                'activity': str(point['activity']),
                'title': 'activity: %s' % point["activity"]
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
        rating = EnergyRating(device_data_rows)
        return_string += str(rating)
        return_string += "<br><br><br>"

    if return_string == "":
        return_string = "No matches found!"

    return return_string

@app.route("/energycertificate")
def energycertificate():

    user_id = session.get('rr_user_id')
    # user_id = 4
    if user_id == None:
        # Not authenticated -> throw back to front page
        return index()

    end_time = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    # end_time = datetime.datetime.strptime("2015-11-18", '%Y-%m-%d')
    start_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)
    ranking_start_time = end_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    end_time_string = end_time.strftime("%Y-%m-%d")
    start_time_string = start_time.strftime("%Y-%m-%d")

    rating, ranking = get_rating(user_id, start_time_string, end_time_string)
    # rating = get_rating(TEMP_FIXED_USER_ID, start_time_string, end_time_string)
    # rating = EnergyRating(TEMP_FIXED_USER_ID)
    # rating.add_on_bicycle_distance(127.84)
    # rating.add_walking_distance(43.2)
    # rating.add_running_distance(55)
    # rating.add_in_mass_transit_A_distance(21.5)
    # rating.add_in_mass_transit_B_distance(27.5)
    # rating.add_in_mass_transit_C_distance(0)
    # rating.add_in_vehicle_distance(35.7)
    # rating.calculate_rating()

    # return svg_generation.generate_energy_rating_svg(rating, start_time, end_time, 1, 2)

    query = '''SELECT past_week_certificates_number FROM global_statistics
               WHERE time < :end_time
               AND time >= :ranking_start_time'''
    ranking_row = db.engine.execute(text(query), ranking_start_time=start_time, end_time=end_time)
    max_ranking = ranking_row.fetchone()["past_week_certificates_number"]

    return svg_generation.generate_energy_rating_svg(rating, start_time, end_time, ranking, max_ranking)


# Helper Functions:

def get_rating(user_id, start_date, end_date):
    query = '''SELECT * FROM travelled_distances
               WHERE time >= :start_date
               AND time < :end_date
               AND user_id = :user_id;
               '''
    distance_rows = db.engine.execute(text(query), start_date=start_date, end_date=end_date, user_id=user_id)
    rows = distance_rows.fetchall()
    ranking = 0
    if len(rows) == 0:
        return EnergyRating(user_id), ranking

    rating = EnergyRating(user_id)
    for row in rows:
        rating.add_travelled_distances_row(row)
        ranking = row["ranking"]
    rating.calculate_rating()
    return rating, ranking


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

    # step1 to acquire one_time_token is done in android client
    try:
        flow = flow_from_clientsecrets(CLIENT_SECRET_FILE,
                                   scope='profile',
                                   redirect_uri=redirect_uri)
        credentials = flow.step2_exchange(one_time_token)
    except FlowExchangeError as err:
        response = make_response(
            json.dumps('Failed to upgrade the authorization code.'), 401)
        response.headers['Content-Type'] = 'application/json'
        # invalid token
        print 'invalid token: ' + one_time_token + ". error: " + err.message
        return response

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
    true_client_id = CLIENT_ID # json.loads(open(CLIENT_SECRET_FILE, 'r').read())['web']['client_id']
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
    if app.debug:
        app.run(host='0.0.0.0')
    else:
        app.run()
