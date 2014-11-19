#!/usr/bin/env python

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
    , device_id integer REFERENCES devices\
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

@app.route('/visualize/<int:device_id>')
def visualize(device_id):
  rows = db.engine.execute(text(
    'SELECT ST_Y(coordinate::geometry), ST_X(coordinate::geometry)\
      FROM device_data\
      WHERE device_id = :device_id\
      ORDER BY time DESC\
      LIMIT 100'), device_id = device_id)
  points = []
  for row in rows:
    points.append({
      'type': 'Feature',
      'geometry': {
        'type': 'Point',
        'coordinates': [ row[1], row[0] ]
      }
    })
  geojson = {
    'type': 'FeatureCollection',
    'features': points
  };
  return render_template('visualize.html',
      api_key = app.config['MAPS_API_KEY'],
      geojson = geojson)

if __name__ == '__main__':
  if app.debug:
    app.run(host = '0.0.0.0')
  else:
    app.run()
