#!/usr/bin/env python

import json
import geoalchemy2 as ga2
from datetime import datetime, date, timedelta
from flask import Flask, abort, jsonify, request, render_template
from flask.ext.sqlalchemy import SQLAlchemy
from sqlalchemy import MetaData, Table, Column, ForeignKey, Integer, String, desc
from sqlalchemy.dialects.postgres import DOUBLE_PRECISION, TIMESTAMP, UUID
from sqlalchemy.exc import DataError
from sqlalchemy.sql import text, func, column, table, select
from uuid import uuid4

app = Flask(__name__)
#app.config.from_pyfile('regularroutes.cfg')
#app.debug = True
app.config.from_envvar('REGULARROUTES_SETTINGS')
db = SQLAlchemy(app)
metadata = MetaData()

# Schema definitions:
devices_table = Table('devices', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('token', UUID, unique=True, nullable=False),
                      Column('created', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                             server_default=func.current_timestamp()),
                      Column('last_activity', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                             server_default=func.current_timestamp()))

device_data_table = Table('device_data', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('device_id', Integer, ForeignKey('devices.id'), nullable=False),
                          Column('coordinate', ga2.Geography('point', 4326), nullable=False),
                          Column('accuracy', DOUBLE_PRECISION, nullable=False),
                          Column('time', TIMESTAMP, nullable=False))

activity_type_table = Table('activity_type', metadata,
                            Column('id', Integer, primary_key=True),
                            Column('activity_type', String(16), nullable=False))

activity_data_table = Table('activity_data', metadata,
                            Column('id', Integer, primary_key=True),
                            Column('activity_type_id', Integer, ForeignKey('activity_type.id'), nullable=False),
                            Column('device_data_id', Integer, ForeignKey('device_data.id'), nullable=False),
                            Column('ordinal', Integer, nullable=False),
                            Column('confidence', Integer))

# Table Creations with initial setup:

if not activity_type_table.exists(bind=db.engine):
    """
        Activity type table is created separately to test if data insertion is required
    """
    activity_type_table.create(bind=db.engine)
    ''' Add type values: IN_VEHICLE, ON_BICYCLE, ON_FOOT, RUNNING, WALKING, STILL, TILTING, UNKNOWN
        These types are the same as are defined in:
        https://developer.android.com/reference/com/google/android/gms/location/DetectedActivity.html
    '''
    activity_types = \
        [{'id': 0, 'activity_type': 'IN_VEHICLE'},
         {'id': 1, 'activity_type': 'ON_BICYCLE'},
         {'id': 2, 'activity_type': 'ON_FOOT'},
         {'id': 3, 'activity_type': 'RUNNING'},
         {'id': 4, 'activity_type': 'STILL'},
         {'id': 5, 'activity_type': 'TILTING'},
         {'id': 6, 'activity_type': 'UNKNOWN'},
         {'id': 7, 'activity_type': 'WALKING'}]

    for activity_type in activity_types:
        stmt = activity_type_table.insert(activity_type)
        db.engine.execute(stmt)

"""
    Other schema tables are created at once, if not exists
"""
metadata.create_all(bind=db.engine, checkfirst=True)


# Methods:
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


def get_device_id(token):

    try:
        query = select([table('devices', column('id'))]).where("token='" + token + "'")
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


@app.route('/register', methods=['POST'])
def register_post():
    token = uuid4()

    insertion = devices_table.insert({'token': token.hex})
    db.engine.execute(insertion)

    return jsonify({
        'deviceToken': token,
        'sessionId': token
    })


@app.route('/authenticate', methods=['POST'])
def authenticate_post():
    device_token = request.form['deviceToken']
    device_id = verify_device_token(device_token)

    update = devices_table.update().values({'last_activity': datetime.now()}).where("id=" + str(device_id))
    db.engine.execute(update)

    return jsonify({
        'sessionId': device_id
    })


@app.route('/data', methods=['POST'])
def data_post():
    session_id = request.args['sessionId']
    device_id = session_id
    json_body = request.json['dataPoints']

    for point in json_body:
        time = datetime.fromtimestamp(long(point['time']) / 1000.0)

        location = point['location']
        coordinate = 'POINT(%f %f)' % (float(location['longitude']), float(location['latitude']))
        accuracy = float(location['accuracy'])

        insertion = device_data_table.insert({
            'device_id': device_id,
            'coordinate': coordinate,
            'accuracy': accuracy,
            'time': time
        })
        result = db.engine.execute(insertion)
        row_id = result.inserted_primary_key[0]

        if row_id is not None:
            print 'Row id: ' + str(row_id)

            activity_data = point['activityData']
            if activity_data is not None:
                activities = activity_data['activities']

                if activities is not None:
                    i = 1
                    for activity in activities:
                        activity_type_id = get_activity_type_id(activity['activityType'])
                        if activity_type_id >= 0:
                            ins = activity_data_table.insert({
                                'activity_type_id': activity_type_id,
                                'device_data_id': row_id,
                                'confidence': activity['confidence'],
                                'ordinal': i
                            })
                            db.engine.execute(ins)
                            i += 1
    return jsonify({
    })


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


@app.route('/device/<device_value>')
def device(device_value):
    print device_value

    try:
        device_id = int(device_value)
        device_token = get_device_token(device_id)

    except ValueError:
        device_token = device_value
        device_id = get_device_id(device_token)
    except Exception as e:
        print 'Exception: ' + e.message

    return jsonify({'device_token': device_token, 'device_id': device_id})


def data_points(device_id, datetime_start, datetime_end):
    return db.engine.execute(text(
        'SELECT id, ST_Y(coordinate::geometry) as longitude, ST_X(coordinate::geometry) as latitude, ST_AsGeoJSON(coordinate) as geojson, coordinate, accuracy, time\
      FROM device_data\
      WHERE device_id = :device_id\
      AND time >= :time_start\
      AND time < :time_end\
      ORDER BY time ASC'
    ), device_id=device_id, time_start=datetime_start, time_end=datetime_end)


@app.route('/visualize/<int:device_id>')
def visualize(device_id):
    return render_template('visualize.html',
                           api_key=app.config['MAPS_API_KEY'],
                           device_id=device_id)


@app.route('/visualize/<int:device_id>/geojson')
def visualize_device_geojson(device_id):
    if 'date' in request.args:
        date_start = datetime.strptime(request.args['date'], '%Y-%m-%d').date()
    else:
        date_start = date.today()

    date_end = date_start + timedelta(days=1)

    points = data_points(device_id, datetime.fromordinal(date_start.toordinal()),
                         datetime.fromordinal(date_end.toordinal()))

    features = []
    links = set()
    waypoints = set()
    for point in points:
        point_geo = json.loads(point['geojson'])
        features.append({
            'type': 'Feature',
            'geometry': point_geo,
            'properties': {
                'type': 'raw-point',
                'title': 'accuracy: %d' % (point['accuracy'])
            }
        })
        if point['accuracy'] < 500:
            link = db.engine.execute(text(
                'SELECT lnk_id, lnk_1, lnk_2, ST_AsGeoJSON(ST_ShortestLine(lnk_geom, :coordinate ::geometry)) as geojson\
          FROM links\
          WHERE lnk_geom && (ST_Buffer(:coordinate ::geography, 500) ::geometry)\
          ORDER BY ST_Distance(lnk_geom, :coordinate ::geometry) ASC\
          LIMIT 1'), coordinate=point['coordinate']).first()
            if link:
                links.add(link['lnk_id'])
                waypoints.add(link['lnk_1'])
                waypoints.add(link['lnk_2'])
                link_geo = json.loads(link['geojson'])
                features.append({
                    'type': 'Feature',
                    'geometry': link_geo,
                    'properties': {
                        'type': 'snap-line'
                    }
                })
    rows = db.engine.execute(text(
        'SELECT ST_AsGeoJSON(geom), wpt_id\
      FROM waypointsclustered'
    ))
    for row in rows:
        if row[1] in waypoints:
            feature_type = 'route-point'
        else:
            feature_type = 'link-point'
        features.append({
            'type': 'Feature',
            'geometry': json.loads(row[0]),
            'properties': {
                'type': feature_type
            }
        })
    rows = db.engine.execute(text(
        'SELECT ST_AsGeoJSON(lnk_geom), lnk_id\
      FROM links'
    ))
    for row in rows:
        if row[1] in links:
            feature_type = 'route-line'
        else:
            feature_type = 'link-line'
        features.append({
            'type': 'Feature',
            'geometry': json.loads(row[0]),
            'properties': {
                'type': feature_type
            }
        })
    geojson = {
        'type': 'FeatureCollection',
        'features': features
    }
    # for query in sorted(get_debug_queries(), key=lambda x: x.duration):
    # print query.statement
    # result = db.engine.execute('EXPLAIN ANALYZE %s' % (query.statement), query.parameters)
    # for row in result:
    #     print row[0]
    #   print '  %s seconds' % (query.duration)
    return jsonify(geojson)


# App starting point:
if __name__ == '__main__':
    if app.debug:
        app.run(host='0.0.0.0')
    else:
        app.run()
