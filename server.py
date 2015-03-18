#!/usr/bin/env python

import json
import hashlib
import geoalchemy2 as ga2
from datetime import date, timedelta
from flask import Flask, abort, jsonify, request, render_template, Response
from flask.ext.sqlalchemy import SQLAlchemy
from oauth2client.client import *
from oauth2client.crypt import AppIdentityError
from sqlalchemy import MetaData, Table, Column, ForeignKey, Enum, BigInteger, Integer, String, Index
from sqlalchemy.dialects.postgres import DOUBLE_PRECISION, TIMESTAMP, UUID
from sqlalchemy.exc import DataError
from sqlalchemy.sql import text, func, column, table, select
from uuid import uuid4

basedir = os.path.abspath(os.path.dirname(__file__))
CLIENT_SECRET_FILE = basedir + '/client_secrets.json'

app = Flask(__name__)
#app.config.from_pyfile('regularroutes.cfg')
#app.debug = True
app.config.from_envvar('REGULARROUTES_SETTINGS')

db = SQLAlchemy(app)
metadata = MetaData()

'''
These types are the same as are defined in:
https://developer.android.com/reference/com/google/android/gms/location/DetectedActivity.html
'''
activity_types = ('IN_VEHICLE', 'ON_BICYCLE', 'ON_FOOT', 'RUNNING', 'STILL', 'TILTING', 'UNKNOWN', 'WALKING')
activity_type_enum = Enum(*activity_types, name='activity_type_enum')

# Schema definitions:
devices_table = Table('devices', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('token', UUID, unique=True, nullable=False),
                      Column('created', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                             server_default=func.current_timestamp()),
                      Column('last_activity', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                             server_default=func.current_timestamp()))

user_auth_table = Table('user_auth', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('device_id', Integer, ForeignKey('devices.id'), nullable=False),
                        Column('google_refresh_token', String, nullable=False),
                        Column('google_server_access_token', String),
                        Column('device_auth_id', String, nullable=False),
                        Column('register_timestamp', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                               server_default=func.current_timestamp()))

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
    old_device_id = data['oldDeviceId']

    # 1. authenticate with Google
    validation_data = authenticate_with_google_oauth(google_one_time_token)
    if validation_data is None:
        abort(403)  # auth failed

    account_google_id = validation_data['google_id']

    # The following hash value is also generated in client and used in authentication
    device_auth_id = str(hashlib.sha256(str(account_google_id).encode('utf-8')).hexdigest()).upper()

    device_id = None
    session_token = None

    # 2. Check if user has registered previously
    ext_device_id = get_device_from_user_auth_id(device_auth_id)
    if ext_device_id >= 0:
        device_id = ext_device_id
        session_token = get_device_token(device_id)

        # 3. Link to old device id, if available (and not registered yet)
    elif old_device_id is not None and old_device_id != '':
        session_token = old_device_id
        device_id = get_device_id(old_device_id)

    if device_id is None:
        session_token = uuid4().hex
        device_insertion = devices_table.insert({'token': session_token})
        db.engine.execute(device_insertion)
        device_id = get_device_id(session_token)

    # 4. create authentication row to db
    if ext_device_id < 0:
        stmt = user_auth_table.insert({'device_id': int(device_id),
                                       'google_refresh_token': str(validation_data['refresh_token']),
                                       'google_server_access_token': str(validation_data['access_token']),
                                       'device_auth_id': str(device_auth_id)})

    else:
        print 're-registration detected -> using existing account'
        stmt = user_auth_table.update().values({'google_refresh_token': str(validation_data['refresh_token']),
                                                'google_server_access_token': str(
                                                    validation_data['access_token'])}).where(
            'device_id={0}'.format(str(ext_device_id)))

    db.engine.execute(stmt)
    resp = jsonify({'sessionToken': session_token})
    return resp


@app.route('/authenticate', methods=['POST'])
def authenticate_post():
    json = request.json
    device_auth_id = json['deviceAuthId']
    device_id = verify_device_auth_id(device_auth_id)

    existing_token = get_device_token(device_id)
    if existing_token is None:
        print 'User is not registered. deviceAuthId=' + device_auth_id
        abort(403)

    update = devices_table.update().values({'last_activity': datetime.datetime.now()}).where("id=" + str(device_id))
    db.engine.execute(update)

    session_token = get_device_token(device_id)

    return jsonify({
        'sessionToken': session_token
    })


@app.route('/data', methods=['POST'])
def data_post():
    session_token = request.args['sessionToken']
    if session_token is None or session_token == '':
        abort(403)  # not authenticated user

    device_id = get_device_id(session_token)
    if device_id < 0:
        abort(403)  # not registered user

    data_points = request.json['dataPoints']

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
        return result

    for chunk in batch_chunks(data_points):
        batch = [prepare_point(x) for x in chunk]
        db.engine.execute(
            device_data_table.insert(batch).returning(device_data_table.c.id))
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
        device_id = get_device_id(session_token)

    except Exception as e:
        print 'Device-query - exception: ' + e.message
        device_id = -1

    if device_id >= 0:
        return jsonify({'sessionToken': session_token, 'deviceId': device_id})
    else:
        return jsonify({'error': "Invalid session token"})


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
        offset = (int(page)-1) * entry_block_size
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


# Helper Functions:


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


def data_points_snapping(device_id, datetime_start, datetime_end):
    return db.engine.execute(text('''
        SELECT id,
            ST_AsGeoJSON(coordinate) AS geojson,
            accuracy,
            activity_1, activity_1_conf,
            activity_2, activity_2_conf,
            activity_3, activity_3_conf,
            waypoint_id
        FROM device_data
        WHERE device_id = :device_id
        AND time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''), device_id=device_id, time_start=datetime_start, time_end=datetime_end)


def verify_device_auth_id(device_auth_id):
    if device_auth_id is None or device_auth_id == '':
        print 'empty device_auth_id'
        abort(403)
    try:
        query = select([table('user_auth', column('device_id'))]).where("device_auth_id='" + device_auth_id + "'")
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
        query = select([table('devices', column('id'))]).where("token='" + token + "'")
        row = db.engine.execute(query).first()

        if not row:
            abort(403)
        return row[0]
    except DataError as e:
        print 'Exception: ' + e.message
        abort(403)


def get_device_id(session_id):
    try:
        query = select([table('devices', column('id'))]).where("token='" + session_id + "'")
        row = db.engine.execute(query).first()
        if not row:
            return -1
        return int(row[0])
    except DataError as e:
        print 'Exception: ' + e.message

    return -1


def get_device_from_user_auth_id(device_auth_id):
    try:
        query = select([table('user_auth', column('device_id'))]).where("device_auth_id='" + device_auth_id + "'")
        row = db.engine.execute(query).first()
        if not row:
            return -1
        return int(row[0])
    except DataError as e:
        print 'Exception: ' + e.message

    return -1


def get_device_token(device_id):
    try:
        query = select([table('devices', column('token'))]).where("id='" + str(device_id) + "'")
        row = db.engine.execute(query).first()
        if not row:
            return None
        return row[0]
    except DataError as e:
        print 'Exception: ' + e.message

    return None


def get_activity_type_id(activity):
    if activity is None:
        return -1

    try:
        query = select([table('activity_type', column('id'))]).where("activity_type='" + activity + "'")
        row = db.engine.execute(query).first()
        if not row:
            return -1
        return int(row[0])
    except DataError as e:
        print 'Exception: ' + e.message

    return -1


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
    if app.debug:
        app.run(host='0.0.0.0')
    else:
        app.run()
