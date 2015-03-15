#!/usr/bin/env python

import json
import itertools
import geoalchemy2 as ga2
from datetime import datetime, date, timedelta
from flask import Flask, abort, jsonify, request, render_template, Response
from flask.ext.sqlalchemy import SQLAlchemy
from sqlalchemy import MetaData, Table, Column, ForeignKey, Enum, Integer, String, Index, desc, subquery
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

'''
These types are the same as are defined in:
https://developer.android.com/reference/com/google/android/gms/location/DetectedActivity.html
'''
activity_types = ('IN_VEHICLE', 'ON_BICYCLE', 'ON_FOOT', 'RUNNING', 'STILL', 'TILTING', 'UNKNOWN', 'WALKING')
activity_type_enum = Enum(*activity_types, name='activity_type_enum')

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
                          Column('time', TIMESTAMP, nullable=False),
                          Column('activity_1', activity_type_enum),
                          Column('activity_1_conf', Integer),
                          Column('activity_2', activity_type_enum),
                          Column('activity_2_conf', Integer),
                          Column('activity_3', activity_type_enum),
                          Column('activity_3_conf', Integer),
                          Index('idx_device_data_time', 'time'),
                          Index('idx_device_data_device_id_time', 'device_id', 'time'))

metadata.create_all(bind=db.engine, checkfirst=True)

# REST interface:


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
    data_points = request.json['dataPoints']

    batch_size = 1024
    def batch_chunks(x):
        for i in xrange(0, len(x), batch_size):
            yield x[i:i+batch_size]

    def prepare_point(point):
        location = point['location']

        result = {
            'device_id': device_id,
            'coordinate': 'POINT(%f %f)' % (float(location['longitude']), float(location['latitude'])),
            'accuracy': float(location['accuracy']),
            'time': datetime.fromtimestamp(long(point['time']) / 1000.0)
        }
        result.update(prepare_point_activities(point))
        return result

    def prepare_point_activities(point):
        if not 'activityData' in point or not 'activities' in point['activityData']:
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
    try:
        device_id = int(device_value)
        device_token = get_device_token(device_id)

    except ValueError:
        device_token = device_value
        device_id = get_device_id(device_token)
    except Exception as e:
        print 'Exception: ' + e.message

    return jsonify({'device_token': device_token, 'device_id': device_id})


massive_advanced_csv_query = """
      SELECT
        device_id, time,
        ST_Y(coordinate::geometry) as longitude,
        ST_X(coordinate::geometry) as latitude,
        accuracy,
        activity_1, activity_1_conf,
        activity_2, activity_2_conf,
        activity_3, activity_3_conf
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
        'SELECT wpt_id,\
            ST_Y(geom::geometry) as longitude,\
            ST_X(geom::geometry) as latitude\
            FROM waypointsclustered')
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
    # print row[0]
    # print '  %s seconds' % (query.duration)
    return jsonify(geojson)


# Helper Functions:


def generate_csv(rows):
    # Poor man's CSV generation. Doesn't handle escaping properly.
    # Python's CSV library doesn't handle unicode, and seems to be
    # tricky to wrap as a generator (expected by Flask)
    yield '"device_id";"time";"longitude";"latitude";"accuracy";"activity_guess_1";"activity_guess_1_conf";"activity_guess_2";"activity_guess_2_conf";"activity_guess_3";"activity_guess_3_conf"\n'

    def to_str(x):
        if x is None:
            return ''
        return str(x)

    for row in rows:
        yield ';'.join(['"%s"' % (to_str(x)) for x in row]) + '\n'


def data_points(device_id, datetime_start, datetime_end):
    return db.engine.execute(text('''
        SELECT id,
            ST_Y(coordinate::geometry) AS longitude,
            ST_X(coordinate::geometry) AS latitude,
            ST_AsGeoJSON(coordinate) AS geojson,
            coordinate, accuracy, time,
            activity_1, activity_1_conf,
            activity_2, activity_2_conf,
            activity_3, activity_3_conf
        FROM device_data
        WHERE device_id = :device_id
        AND time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''), device_id=device_id, time_start=datetime_start, time_end=datetime_end)

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

# App starting point:
if __name__ == '__main__':
    if app.debug:
        app.run(host='0.0.0.0')
    else:
        app.run()
