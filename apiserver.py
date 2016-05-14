#!/usr/bin/env python

from uuid import uuid4

from flask import Flask, abort, jsonify, request
from oauth2client.client import *
from sqlalchemy.sql import and_, func, select, text
import json

from pyfiles.common_helpers import simplify, trace_linestrings

from pyfiles.database_interface import init_db, db_engine_execute, users_table_insert, users_table_update, devices_table_insert, device_data_table_insert
from pyfiles.database_interface import verify_user_id, update_last_activity, get_users_table_id_for_device, get_device_table_id
from pyfiles.database_interface import get_device_table_id_for_session, get_users_table_id, get_session_token_for_device, get_user_id_from_device_id
from pyfiles.database_interface import activity_types
from pyfiles.database_interface import get_svg

from pyfiles.authentication_helper import user_hash, authenticate_with_google_oauth

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

db, store = init_db(app)

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
    validation_data = authenticate_with_google_oauth(app.config['AUTH_REDIRECT_URI'], CLIENT_SECRET_FILE, CLIENT_ID, google_one_time_token)
    if validation_data is None:
        abort(403)  # auth failed

    account_google_id = validation_data['google_id']

    # The following hash value is also generated in client and used in authentication
    user_id = user_hash(account_google_id)

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
        users_table_id = users_table_insert(str(user_id), str(validation_data['refresh_token']), str(validation_data['access_token']))
    else:
        print 're-registration for same user detected -> using existing user account'
        users_table_update(str(users_table_id), str(validation_data['refresh_token']), str(validation_data['access_token']))

    # 5. Create/update device to db
    if devices_table_id is None:
        session_token = uuid4().hex
        devices_table_insert(users_table_id, device_id, installation_id, device_model, session_token)
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
        device_data_table_insert(batch)
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
        rows = db_engine_execute(query.compile())

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
        result = db_engine_execute(text(sql))
        return '%d duplicate data points were deleted' % (result.rowcount)


@app.route('/maintenance/snapping')
def maintenance_snapping():
    with open('sql/snapping.sql', 'r') as sql_file:
        sql = sql_file.read()
        result = db_engine_execute(text(sql))
        return 'Snapping was done to %d data points' % (result.rowcount)


@app.route('/path/<session_token>')
def path(session_token):
    # get data for specified date, or last 12h if unspecified
    date = request.args.get("date")

    # passed on to simplify
    maxpts = int(request.args.get("maxpts") or 0)
    mindist = int(request.args.get("mindist") or 0)

    if date:
        start = datetime.datetime.strptime(date, '%Y-%m-%d').replace(
            hour=0, minute=0, second=0, microsecond=0)
    else:
        start = datetime.datetime.now() - datetime.timedelta(hours=12)
    end = start + datetime.timedelta(hours=24)

    filtered_data = db.metadata.tables["device_data_filtered"]
    device_data = db.metadata.tables["device_data"]
    devices = db.metadata.tables["devices"]
    users = db.metadata.tables["users"]

    # use filtered data if available
    query = select(
        [   func.ST_AsGeoJSON(filtered_data.c.coordinate).label("geojson"),
            filtered_data.c.activity,
            filtered_data.c.line_type,
            filtered_data.c.time],
        and_(
            devices.c.token == session_token,
            filtered_data.c.time >= start,
            filtered_data.c.time <= end),
        filtered_data.join(users).join(devices),
        order_by=filtered_data.c.time)
    points = db.engine.execute(query).fetchall()
    if points:
        start = points[-1]["time"]

    # fall back on unfiltered for the missing part
    query = select(
        [   func.ST_AsGeoJSON(device_data.c.coordinate).label("geojson"),
            device_data.c.activity_1.label("activity")],
        and_(
            devices.c.token == session_token,
            device_data.c.time > start,
            device_data.c.time <= end),
        device_data.join(devices),
        order_by=device_data.c.time)
    points += db.engine.execute(query)

    points = simplify(points, maxpts=maxpts, mindist=mindist)
    geojson = {
        'type': 'FeatureCollection',
        'features': list(trace_linestrings(points))
    }

    return jsonify(geojson)


@app.route('/svg/<session_token>')
def svg(session_token):
    device_id = get_device_table_id_for_session(session_token)
    if device_id < 0:
        return ""
    user_id = get_user_id_from_device_id(device_id)
    if user_id < 0:
        return ""
    return get_svg(user_id)

# App starting point:
if __name__ == '__main__':
    if app.debug:
        app.run(host='0.0.0.0')
    else:
        app.run()
