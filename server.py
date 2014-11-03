#!/usr/bin/env python

from flask import Flask, abort, jsonify, request

app = Flask(__name__)

hardcoded_token = 'topsecrettoken'

def verify_token():
  token = request.form['token']
  if token != hardcoded_token:
    abort(401)

@app.route('/authenticate', methods = ['POST'])
def authenticate_post():
  device_id = request.form['id']
  return jsonify({
    'token': hardcoded_token
  })

@app.route('/data', methods = ['POST'])
def data_post():
  verify_token()
  return jsonify({
  })

if __name__ == '__main__':
  app.run(debug = True)
