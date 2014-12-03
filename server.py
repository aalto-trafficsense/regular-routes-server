#!/usr/bin/env python

import json
from datetime import datetime
from flask import Flask, abort, jsonify, request, render_template
from flask.ext.sqlalchemy import SQLAlchemy
from sqlalchemy.sql import text
from uuid import uuid4

app = Flask(__name__)
app.config.from_envvar('REGULARROUTES_SETTINGS')
db = SQLAlchemy(app)

db.engine.execute(text(
  'CREATE TABLE IF NOT EXISTS devices\
    ( id serial PRIMARY KEY\
    , token uuid UNIQUE NOT NULL\
    , created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP\
    , last_activity timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP\
    )'
))
db.engine.execute(text(
  'CREATE TABLE IF NOT EXISTS device_data\
    ( id serial PRIMARY KEY\
    , device_id integer REFERENCES devices NOT NULL\
    , coordinate geography(point, 4326) NOT NULL\
    , accuracy double precision NOT NULL\
    , time timestamp NOT NULL\
    )'
))

def verify_device_token(token):
  row = db.engine.execute(text('SELECT id FROM devices WHERE token = :token'), token = token).first()
  if not row:
    abort(403)
  return row[0]

@app.route('/register', methods = ['POST'])
def register_post():
  token = uuid4()
  db.engine.execute(text(
    'INSERT INTO devices\
      (token, created, last_activity)\
      VALUES\
      (:token, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)'), token = token.hex)
  return jsonify({
    'deviceToken': token,
    'sessionId': token
  })

@app.route('/authenticate', methods = ['POST'])
def authenticate_post():
  device_token = request.form['deviceToken']
  device = verify_device_token(device_token)
  db.engine.execute(text(
    'UPDATE devices SET \
      last_activity = CURRENT_TIMESTAMP\
      WHERE id = :id'), id = device)
  return jsonify({
    'sessionId': device
  })

@app.route('/data', methods = ['POST'])
def data_post():
  session_id = request.args['sessionId']
  device = session_id
  for point in request.json['dataPoints']:
    time = datetime.fromtimestamp(long(point['time']) / 1000.0)

    location = point['location']
    coordinate = 'POINT(%f %f)' % (float(location['longitude']), float(location['latitude']))
    accuracy = float(location['accuracy'])

    db.engine.execute(text(
      'INSERT INTO device_data\
        (device_id, coordinate, accuracy, time)\
        VALUES\
        (:device, :coordinate, :accuracy, :time)'),
      device = device,
      coordinate = coordinate,
      accuracy = accuracy,
      time = time
    )
  return jsonify({
  })

@app.route('/devices')
def devices():
  rows = db.engine.execute(text(
    'SELECT id, token\
      FROM devices\
      ORDER BY token DESC'))
  result = ""
  for row in rows:
    result += '%s = %s\n' % (row[1], row[0])
  return str(result)

def next_data_point(device_id, time = None):
  if time:
    query = text(
      'SELECT id, ST_Y(coordinate::geometry) as longitude, ST_X(coordinate::geometry) as latitude, ST_AsGeoJSON(coordinate) as geojson, coordinate, accuracy, time\
        FROM device_data\
        WHERE device_id = :device_id\
        AND time > :time\
        ORDER BY time ASC\
        LIMIT 1'
    )
  else:
    query = text(
      'SELECT id, ST_Y(coordinate::geometry) as longitude, ST_X(coordinate::geometry) as latitude, ST_AsGeoJSON(coordinate) as geojson, coordinate, accuracy, time\
        FROM device_data\
        WHERE device_id = :device_id\
        ORDER BY time ASC\
        LIMIT 1'
    )
  return db.engine.execute(query, device_id = device_id, time = time).first()

@app.route('/visualize/<int:device_id>')
def visualize(device_id):
  return render_template('visualize.html',
      api_key = app.config['MAPS_API_KEY'],
      device_id = device_id)

@app.route('/visualize/<int:device_id>/geojson')
def visualize_geojson(device_id):
  point = next_data_point(device_id)
  prev_point = None
  features = []
  links = set()
  waypoints = set()
  while point:
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
          LIMIT 1'), coordinate = point['coordinate']).first()
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
    point = next_data_point(device_id, point['time'])
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
  };
  return jsonify(geojson);

if __name__ == '__main__':
  if app.debug:
    app.run(host = '0.0.0.0')
  else:
    app.run()
