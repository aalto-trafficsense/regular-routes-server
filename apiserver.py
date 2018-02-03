#!/usr/bin/env python

import datetime
import json
import os
from uuid import uuid4

from flask import Flask, abort, jsonify, request, make_response

from sqlalchemy.sql import (
    and_, func, not_, select)

from pyfiles.common_helpers import stop_clusters

from pyfiles.constants import (
    DEST_RADIUS_MAX,
    DESTINATIONS_LIMIT,
    INCLUDE_DESTINATIONS_BETWEEN,
    int_activities)

from pyfiles.database_interface import (init_db, users_table_insert, users_table_update, devices_table_insert, device_data_table_insert,
                                        device_location_table_insert, device_activity_table_insert, verify_user_id, update_last_activity,
                                        update_messaging_token, get_device_table_id, get_device_table_id_for_session, get_users_table_id,
                                        get_session_token_for_device, get_user_id_from_device_id, activity_types, client_log_table_insert,
                                        get_svg)

from pyfiles.server_common import common_setlegmode, common_path


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
        Server should receive valid one-time token that can be used to authenticate user via Google+ API
        See: https://developers.google.com/+/web/signin/server-side-flow#step_6_send_the_authorization_code_to_the_server

    """
    data = request.json
    google_one_time_token = data['oneTimeToken']
    device_id = data['deviceId']
    installation_id = data['installationId']
    device_model = data['deviceModel']
    # print 'deviceModel=' + str(device_model)
    # Get optionally - old clients do not send this
    client_version = data.get('clientVersion')

    # 1. authenticate with Google
    validation_data = authenticate_with_google_oauth(app.config['AUTH_REDIRECT_URI'], CLIENT_SECRET_FILE, CLIENT_ID, google_one_time_token)
    if validation_data is None:
        abort(403)  # auth failed

    account_google_id = validation_data['google_id']

    # The following hash value is also generated in client and used in authentication
    user_id = user_hash(account_google_id)

    users_table_id = None

    # 2. Check if user has registered previously
    ext_users_table_id = get_users_table_id(user_id)
    if ext_users_table_id >= 0:
        users_table_id = ext_users_table_id

    # 3. Check if same device has registered with same user
    devices_table_id = get_device_table_id(
        users_table_id, device_id, installation_id)

    # 4. create/update user to db
    if users_table_id is None:
        users_table_id = users_table_insert(str(user_id), str(validation_data['refresh_token']), str(validation_data['access_token']))
    else:
        print 're-registration for same user detected -> using existing user account'
        users_table_update(str(users_table_id), str(validation_data['refresh_token']), str(validation_data['access_token']))

    # 5. Create/update device to db
    if devices_table_id is None:
        session_token = uuid4().hex
        devices_table_id = devices_table_insert(
            users_table_id,
            device_id,
            installation_id,
            device_model,
            session_token,
            client_version)
    else:
        print 'device re-registration detected -> using same device'
        update_last_activity(devices_table_id, client_version)
        session_token = get_session_token_for_device(devices_table_id)

    # 6. Log the registration
    client_log_table_insert(
        devices_table_id,
        users_table_id,
        "MOBILE-REGISTER",
        "ClientVersion:" + (client_version or ""))

    resp = jsonify({'sessionToken': session_token})
    return resp


@app.route('/authenticate', methods=['POST'])
def authenticate_post():
    json = request.json
    user_id = json['userId']
    device_id = json['deviceId']
    installation_id = json['installationId']
    # Get optionally - old clients do not send this
    client_version = json.get('clientVersion')
    messaging_token = json.get('messagingToken', '')

    # 1. check that user exists or abort
    users_table_id = verify_user_id(user_id)

    devices_table_id = get_device_table_id(
        users_table_id, device_id, installation_id)
    session_token = get_session_token_for_device(devices_table_id)
    if session_token is None:
        print 'User is not registered. userId=' + user_id
        abort(403)

    # 2. Update messaging token, if included
    if len(messaging_token) > 1:
        update_messaging_token(devices_table_id, messaging_token)
    update_last_activity(devices_table_id, client_version)

    # 3. Update log
    client_log_table_insert(
        devices_table_id,
        get_user_id_from_device_id(devices_table_id),
        "MOBILE-AUTHENTICATE",
        "ClientVersion:" + (client_version or ""))

    return jsonify({
        'sessionToken': session_token
    })


@app.route('/msgtokenrefresh/<session_token>')
def fbrefresh(session_token):
    device_id = get_device_table_id_for_session(session_token)
    if device_id < 0:
        return ""
    # get the token
    messaging_token = request.args.get("messaging_token")
    if len(messaging_token) > 1:
        update_messaging_token(device_id, messaging_token)

    user_id = get_user_id_from_device_id(device_id)
    if user_id < 0:
        return ""
    client_log_table_insert(device_id, user_id, "MOBILE-FCM-TOKEN", "")
    return make_response(json.dumps('Ack'), 200)


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


@app.route('/locationdata', methods=['POST'])
def location_post():
    session_token = request.args['sessionToken']
    if session_token is None or session_token == '':
        abort(403)  # not authenticated user

    device_id = get_device_table_id_for_session(session_token)
    if device_id < 0:
        abort(403)  # not registered user

    data = request.json
    # Process locations
    data_points = data['locations']
    # Remember, if a single point fails, the whole batch fails
    batch_size = 1024

    def batch_chunks(x):
        for i in xrange(0, len(x), batch_size):
            yield x[i:i + batch_size]

    def prepare_point(point):
        result = {
            'device_id': device_id,
            'coordinate': 'POINT(%f %f)' % (float(point['longitude']), float(point['latitude'])),
            'accuracy': float(point['accuracy']),
            'time': datetime.datetime.fromtimestamp(long(point['time']) / 1000.0)
        }
        return result

    for chunk in batch_chunks(data_points):
        batch = [prepare_point(x) for x in chunk]
        device_location_table_insert(batch)

    # Process activities
    activityEntries = data.get('activityEntries') # Optional - not included e.g. while using the simulator

    if activityEntries:
        def prepare_activity(activitydata):
            activities = activitydata.get('activities')

            def parse_activities():
                for activity in activities:
                    yield {
                        'type': int_activities[int(activity['activity'])],
                        'confidence': int(activity['confidence'])
                    }

            result = {}
            if activities:

                sorted_activities = sorted(parse_activities(), key=lambda x: x['confidence'], reverse=True)
                result['device_id'] = device_id
                result['time'] = datetime.datetime.fromtimestamp(long(activitydata['time']) / 1000.0)

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

        for chunk in batch_chunks(activityEntries):
            batch = [prepare_activity(x) for x in chunk]
            device_activity_table_insert(batch)


    return jsonify({
    })

@app.route('/datav2', methods=['POST'])
def datav2_post():
    session_token = request.args['sessionToken']
    if session_token is None or session_token == '':
        abort(403)  # not authenticated user

    device_id = get_device_table_id_for_session(session_token)
    if device_id < 0:
        abort(403)  # not registered user

    data = request.json
    # Process locations
    data_points = data['locations']
    activityEntries = data.get('activityEntries') # Optional - not included e.g. while using the simulator
    # Remember, if a single point fails, the whole batch fails
    batch_size = 1024
    class prevTime: pass
    prevTime.x = -1
    class actFollow: pass
    actFollow.index = 0

    def batch_chunks(x):
        for i in xrange(0, len(x), batch_size):
            yield x[i:i + batch_size]

    def prepare_point(point):
        loc_time = point['time']
        if loc_time != prevTime.x:
            prevTime.x = loc_time
            result = {
                'device_id': device_id,
                'coordinate': 'POINT(%f %f)' % (float(point['longitude']), float(point['latitude'])),
                'accuracy': float(point['accuracy']),
                'time': datetime.datetime.fromtimestamp(long(loc_time) / 1000.0)
            }
            if activityEntries:
                continue_loop = True
                actFollow.last_element = False
                class minInterval: pass
                minInterval.x = 999999999
                while continue_loop:
                    act_time = activityEntries[actFollow.index]['time']
                    interval = abs(loc_time - act_time)
                    # print "testing index: " + str(actFollow.index) + " interval: " + str(interval) + " minInterval: " + str(minInterval.x)
                    if interval < minInterval.x:
                        minInterval.x = interval
                        actFollow.index += 1
                        if actFollow.index >= len(activityEntries):
                            actFollow.last_element = True
                    if (interval > minInterval.x) | (actFollow.last_element == True):
                        continue_loop = False
                        actFollow.index -= 1  # back up one
                        if minInterval.x > 60000:  # Reject locations with no activity info
                            print "/datav2 skipping location due to no activity, minInterval: " + str(minInterval.x)
                            result = None
                        else:
                            actEntry = activityEntries[actFollow.index]
                            # print "Best match index: " + str(actFollow.index)
                            activities = actEntry.get('activities')

                            def parse_activities():
                                for activity in activities:
                                    yield {
                                        'type': int_activities[int(activity['activity'])],
                                        'confidence': int(activity['confidence'])
                                    }

                            if activities:

                                sorted_activities = sorted(parse_activities(), key=lambda x: x['confidence'],
                                                           reverse=True)

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
        batch = []
        for x in chunk:
            res = prepare_point(x)
            if res: batch.append(res)
        device_data_table_insert(batch)

    return jsonify({
    })


@app.route('/destinations/<session_token>')
def destinations(session_token):

    dd = db.metadata.tables["device_data"]
    devices = db.metadata.tables["devices"]
    users = db.metadata.tables["users"]
    legs = db.metadata.tables["legs"]

    # Limit number of destinations on output, all if blank given
    limit = request.args.get("limit", DESTINATIONS_LIMIT)
    limit = None if limit == "" else int(limit)

    # Exclude nearby and faraway destinations from point if given, or last
    # device location is not given. All included if blank given in either.
    lat = request.args.get("lat")
    lng = request.args.get("lng")

    exclude = True
    if "" not in (lat, lng):
        if None not in (lat, lng):
            excoord = "POINT(%s %s)" % (lng, lat)
        else:
            excoord = db.engine.execute(select(
                [func.ST_AsText(dd.c.coordinate)],
                devices.c.token == session_token,
                order_by=dd.c.time.desc(),
                limit=1)).scalar()
        if excoord is not None:
            rmin, rmax = INCLUDE_DESTINATIONS_BETWEEN
            exclude = and_(
                not_(func.st_dwithin(legs.c.coordinate_start, excoord, rmin)),
                func.st_dwithin(legs.c.coordinate_start, excoord, rmax))

    start = datetime.datetime.now() - datetime.timedelta(days=30)

    query = select(
        [   func.ST_AsGeoJSON(legs.c.coordinate_start).label("geojson"),
            legs.c.time_start,
            legs.c.time_end],
        and_(
            devices.c.token == session_token,
            legs.c.time_end >= start,
            legs.c.activity == "STILL",
            exclude),
        devices.join(users).join(legs))

    stops = list(dict(x) for x in db.engine.execute(query))
    for x in stops:
        x["coordinates"] = json.loads(x["geojson"])["coordinates"]

    dests = sorted(
        stop_clusters(stops, DEST_RADIUS_MAX * 2),
        key=lambda x: x["visits_rank"])

    geojson = {
        "type": "FeatureCollection",
        "features": [
            {   "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": d["coordinates"]},
                "properties": d}
            for d in dests[:limit]]}

    for f in geojson["features"]:
        del f["properties"]["coordinates"] # included in geometry
        f["properties"]["visits"] = len(f["properties"]["visits"])

    devices_table_id = get_device_table_id_for_session(session_token)
    client_log_table_insert(devices_table_id, get_user_id_from_device_id(devices_table_id), "MOBILE-DESTINATIONS", "")

    return jsonify(geojson)

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


@app.route('/path/<session_token>')
def path(session_token):
    devices_table_id = get_device_table_id_for_session(session_token)
    client_log_table_insert(
        devices_table_id,
        get_user_id_from_device_id(devices_table_id),
        "MOBILE-PATH",
        request.args.get("date"))
    devices = db.metadata.tables["devices"]
    where = devices.c.token == session_token
    return common_path(request, db, where)


@app.route('/setlegmode', methods=['POST'])
def setlegmode_post():
    """Allow user to correct detected transit modes and line names."""

    devices = db.metadata.tables["devices"]
    user = select(
        [devices.c.user_id],
        devices.c.token == request.json["sessionToken"]
        ).scalar()

    device, legid, legact, legline = common_setlegmode(request, db, user)

    client_log_table_insert(
        device,
        user,
        "MOBILE-PATH-EDIT",
        "%s %s %s" % (legid, legact, legline))

    return jsonify({})


@app.route('/svg/<session_token>')
def svg(session_token):
    device_id = get_device_table_id_for_session(session_token)
    if device_id < 0:
        return ""
    user_id = get_user_id_from_device_id(device_id)
    if user_id < 0:
        return ""

    firstlastday = [
        d in request.args and datetime.datetime.strptime(
            request.args[d], '%Y-%m-%d') or None
        for d in ["firstday", "lastday"]]

    client_log_table_insert(
        device_id,
        user_id,
        "MOBILE-CERTIFICATE",
        "/".join(str(x)[:10] for x in firstlastday))

    return get_svg(user_id, *firstlastday)




# App starting point:
if __name__ == '__main__':
    if app.debug:
        app.run(host='0.0.0.0')
    else:
        app.run()
