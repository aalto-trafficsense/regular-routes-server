#!/usr/bin/env python

from datetime import timedelta

from flask import Flask, jsonify, request, render_template, Response
from oauth2client.client import *
from sqlalchemy.sql import text

from pyfiles.common_helpers import simplify_geometry, trace_linestrings
from pyfiles.database_interface import init_db, db_engine_execute, data_points_snapping
from pyfiles.prediction.run_prediction import predict

import json

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

# REST interface developer functions

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
    rows = db_engine_execute(query)
    return Response(generate_csv_waypoints(rows), mimetype='text/csv')


def generate_csv_waypoints(rows):
    yield '"wpt_id";"longitude";"latitude"\n'
    for row in rows:
        yield ';'.join(['"%s"' % (str(x)) for x in row]) + '\n'


@app.route('/waypoints')
def map_waypoints():
    return render_template('waypoints.html', api_key=app.config['MAPS_API_KEY'])


@app.route('/waypoints/geojson')
def map_waypoints_geojson():
    gridsz = 3 # arbitrary
    maxpts = 1000 # arbitrary ...ish, finishes in <10s on some machine

    bounds_arg = json.loads(request.args['bounds'])
    lat0 = bounds_arg['south']
    lat1 = bounds_arg['north']
    lon0 = bounds_arg['west']
    lon1 = bounds_arg['east']
    bounds = {'lat0': lat0, 'lat1': lat1, 'lon0': lon0, 'lon1': lon1}

    # get point counts in grid
    res = list(db.engine.execute(
        text('''
            SELECT * FROM ( -- no alias in HAVING with pg...
            SELECT count(id) npt,
                width_bucket(ST_Y(geo::geometry), :lat0, :lat1, :gridsz) wby,
                width_bucket(ST_X(geo::geometry), :lon0, :lon1, :gridsz) wbx
            FROM waypoints
            GROUP BY wby, wbx) t
            WHERE wby > 0 AND wby <= :gridsz AND wbx > 0 AND wbx <= :gridsz'''),
        gridsz=gridsz,
        **bounds))

    # get all points in one query if no groups needed
    unclustered = [bounds]
    features = []

    # if too many points, group where needed
    if sum(row['npt'] for row in res) > maxpts:
        unclustered = []
        for row in res:
            if row['npt'] > maxpts / gridsz / gridsz:
                features.append({
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [
                            lon0+(lon1-lon0)/gridsz*(row['wbx']-.5),
                            lat0+(lat1-lat0)/gridsz*(row['wby']-.5)
                    ]},
                    'properties': {
                        'type': 'route-point-cluster',
                        'title': "%i points" % row['npt']
                }})
            else:
                unclustered.append({
                    'lat0': lat0+(lat1-lat0)/gridsz*(row['wby']-1),
                    'lat1': lat0+(lat1-lat0)/gridsz*row['wby'],
                    'lon0': lon0+(lon1-lon0)/gridsz*(row['wbx']-1),
                    'lon1': lon0+(lon1-lon0)/gridsz*row['wbx']})

    # query up to gridsz^2 - 1 times, not that many but kinda bummer
    for cell in unclustered:
        res = db.engine.execute(
            text('''
                SELECT id, ST_AsGeoJSON(geo) geojson
                FROM waypoints
                WHERE
                    ST_Y(geo::geometry) >= :lat0 AND
                    ST_Y(geo::geometry) <= :lat1 AND
                    ST_X(geo::geometry) >= :lon0 AND
                    ST_X(geo::geometry) <= :lon1'''),
            **cell)
        for row in res:
            features.append({
                'type': 'Feature',
                'geometry': json.loads(row['geojson']),
                'properties': {
                    'type': 'route-point',
                    'title': 'id: %i' % row['id'] }
            })
    geojson = {
        'type': 'FeatureCollection',
        'features': features
    }
    return jsonify(geojson)


@app.route('/predictgeojson/<int:device_id>')
def predictgeojson(device_id):
    return jsonify(predict(device_id,False)) # False = production server, True = test server

@app.route('/predict/<int:device_id>')
def predict_dev(device_id):
    return render_template('predict.html',
                           api_key=app.config['MAPS_API_KEY'],
                           device_id=device_id)

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

    # these args can be given to test simplify on the linestring rendering
    maxpts = int(request.args.get("maxpts") or 0)
    mindist = int(request.args.get("mindist") or 0)

    points = data_points_snapping(device_id, date_start, date_end).fetchall()

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
                'title': 'accuracy: %d\n%s\n%s' % (point['accuracy'], activity_info, str(point['time']))
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

    simplified = [
        dict(x) for x in simplify_geometry(
            points, maxpts=maxpts, mindist=mindist)]
    for p in simplified:
        p['activity'] = p['activity_1']
    features += trace_linestrings(
        simplified, ('activity',), {'type': 'trace-line'})

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

# App starting point:
if __name__ == '__main__':
    if app.debug:
        app.run(host='0.0.0.0')
    else:
        app.run()
