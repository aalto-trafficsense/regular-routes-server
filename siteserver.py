#!/usr/bin/env python

import random
import string
from datetime import timedelta

from flask import Flask, jsonify, request, render_template
from flask import make_response, session
from oauth2client.client import *
from sqlalchemy.sql import text

from pyfiles import svg_generation
from pyfiles.database_interface import init_db, get_rating, data_points_filtered, get_users_table_id
from pyfiles.authentication_helper import user_hash, verify_and_get_account_id

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

# TrafficSense website REST interface:

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

@app.route('/signin', methods=['GET'])
def sign_in():
    return index()


@app.route('/connect', methods=['POST'])
def connect():
  """Exchange the one-time authorization code for a token and
  store the token in the session."""
  # Ensure that the request is not a forgery and that the user sending
  # this connect request is the expected user.
  # print 'Session state returns: ' + session.get('state')
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
  google_id = verify_and_get_account_id(CLIENT_ID, credentials)['google_id']

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
  user_hash_id = user_hash(google_id)
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
    return render_template('menu.html',
                           RR_URL_PREFIX=app.config['RR_URL_PREFIX'])

@app.route('/nodata')
def no_data():
    """No data was found for this user account."""
    return render_template('nodata.html')

@app.route('/energymap')
def energymap():
    """Draw the energy consumption map of the user."""
    user_id = session.get('rr_user_id')
    if user_id == None:
        # Not authenticated -> throw back to front page
        return index()
    return render_template('energymap.html',
                           RR_URL_PREFIX=app.config['RR_URL_PREFIX'],
                           api_key=app.config['MAPS_API_KEY'])


@app.route('/energymap/geojson')
def energymap_device_geojson():
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

# App starting point:
if __name__ == '__main__':
    if app.debug:
        app.run(host='0.0.0.0')
    else:
        app.run()
