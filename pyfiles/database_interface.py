#!/usr/bin/env python

import geoalchemy2 as ga2
from flask import abort
from flask.ext.sqlalchemy import SQLAlchemy
from oauth2client.client import *
from sqlalchemy import MetaData, Table, Column, ForeignKey, Enum, BigInteger, Integer, String, Index, UniqueConstraint, \
    Float
from sqlalchemy.dialects.postgres import DOUBLE_PRECISION, TIMESTAMP, UUID
from sqlalchemy.exc import DataError
from sqlalchemy.sql import text, func, column, table, select
import svg_generation
from datetime import timedelta

from pyfiles.energy_rating import EnergyRating
import json

from constants import *


# from simplekv.memory import DictStore
from simplekv.db.sql import SQLAlchemyStore
from flask_kvsession import KVSessionExtension

import logging
logging.basicConfig()

'''
These types are the same as are defined in:
https://developer.android.com/reference/com/google/android/gms/location/DetectedActivity.html
'''
activity_types = ('IN_VEHICLE', 'ON_BICYCLE', 'ON_FOOT', 'RUNNING', 'STILL', 'TILTING', 'UNKNOWN', 'WALKING')
activity_type_enum = Enum(*activity_types, name='activity_type_enum')

'''
These types are the same as here:
https://github.com/HSLdevcom/navigator-proto/blob/master/src/routing.coffee#L43
'''
mass_transit_types = ("FERRY", "SUBWAY", "TRAIN", "TRAM", "BUS")
mass_transit_type_enum = Enum(*mass_transit_types, name='mass_transit_type_enum')

# Declare the db and tables global, so they can be referenced from outside the ini_db procedure
db = None
users_table = None
devices_table = None
device_data_table = None
device_data_filtered_table = None
travelled_distances_table = None
mass_transit_data_table = None
global_statistics_table = None


def init_db(app):
    global db
    db = SQLAlchemy(app)

    # chicken, meet egg
    metadata = db.metadata
    metadata.bind = db.engine

    # Run session storage also on SQLAlchemy
    store = SQLAlchemyStore(db.engine, metadata, 'kv_session')
    KVSessionExtension(store, app)

    # Schema definitions:

    # Table with one entry for each user
    global users_table
    users_table = Table('users', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('user_id', String, unique=True, nullable=False),  # hash value of Google Id
                        Column('google_refresh_token', String),
                        Column('google_server_access_token', String),
                        Column('register_timestamp', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                               server_default=func.current_timestamp()),
                        Index('idx_users_user_id', 'user_id'))

    if not users_table.exists():
        users_table.create()
        """ create legacy user that can be used to link existing data that was added
            before user objects were added to some user.
        """
        db.engine.execute(users_table.insert({'id': 0, 'user_id': 'legacy-user'}))

    # Table with one entry for each client device
    global devices_table
    devices_table = Table('devices', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('user_id', Integer, ForeignKey('users.id'), nullable=False, default=0),
                          Column('device_id', String, nullable=False),
                          Column('installation_id', UUID, nullable=False),
                          Column('device_model', String, default='(unknown)'),
                          Column('token', UUID, unique=True, nullable=False),
                          Column('created', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                                 server_default=func.current_timestamp()),
                          Column('last_activity', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                                 server_default=func.current_timestamp()),
                          UniqueConstraint('device_id', 'installation_id', name='uix_device_id_installation_id'),
                          Index('idx_devices_device_id_inst_id', 'device_id', 'installation_id'))

    global device_data_table
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

    Index('idx_device_data_snapping_time_null', device_data_table.c.snapping_time, postgresql_where=device_data_table.c.snapping_time == None)

    # device_data_table after filtering activities.
    global device_data_filtered_table
    device_data_filtered_table = Table('device_data_filtered', metadata,
                              Column('id', Integer, primary_key=True),
                              Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
                              Column('coordinate', ga2.Geography('point', 4326, spatial_index=False), nullable=False),
                              Column('time', TIMESTAMP, nullable=False),
                              Column('activity', activity_type_enum),
                              Column('waypoint_id', BigInteger),
                              Column('line_type', mass_transit_type_enum),
                              Column('line_name', String),
                              Index('idx_device_data_filtered_time', 'time'),
                              Index('idx_device_data_filtered_user_id_time', 'user_id', 'time'))

    device_data_filtered_table.create(checkfirst=True)

    # travelled distances per day per device
    global travelled_distances_table
    travelled_distances_table = Table('travelled_distances', metadata,
                              Column('id', Integer, primary_key=True),
                              Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
                              Column('time', TIMESTAMP, nullable=False), #Only the date portion of time is used. TIMESTAMP datatype used for consistency.
                              Column('cycling', Float),
                              Column('walking', Float),
                              Column('running', Float),
                              Column('mass_transit_a', Float),
                              Column('mass_transit_b', Float),
                              Column('mass_transit_c', Float),
                              Column('car', Float),
                              Column('total_distance', Float),
                              Column('average_co2', Float),
                              Column('ranking', Integer),
                              UniqueConstraint('time', 'user_id', name="unique_user_id_and_time_on_travelled_distances"),
                              Index('idx_travelled_distances_time', 'time'),
                              Index('idx_travelled_distances_device_id_time', 'user_id', 'time'))
    if not travelled_distances_table.exists():
        travelled_distances_table.create(checkfirst=True)
        db.engine.execute(text('''
            CREATE RULE "travelled_distances_table_duplicate_update" AS ON INSERT TO "travelled_distances"
            WHERE EXISTS(SELECT 1 FROM travelled_distances
                        WHERE (user_id, time)=(NEW.user_id, NEW.time))
            DO INSTEAD UPDATE travelled_distances
                SET ranking = NEW.ranking
                WHERE user_id = NEW.user_id
                AND time = NEW.time;
            '''))


    # HSL mass transit vehicle locations.
    global mass_transit_data_table
    mass_transit_data_table = Table('mass_transit_data', metadata,
                              Column('id', Integer, primary_key=True),
                              Column('coordinate', ga2.Geography('point', 4326, spatial_index=True), nullable=False),
                              Column('time', TIMESTAMP, nullable=False),
                              Column('line_type', mass_transit_type_enum, nullable=False),
                              Column('line_name', String, nullable=False),
                              Column('vehicle_ref', String, nullable=False),
                              UniqueConstraint('time', 'vehicle_ref', name="unique_vehicle_and_timestamp"),
                              Index('idx_mass_transit_data_time', 'time'),
                              Index('idx_mass_transit_data_time_coordinate', 'time', 'coordinate'))


    if not mass_transit_data_table.exists():
        mass_transit_data_table.create(checkfirst=True)
        db.engine.execute(text('''
            CREATE RULE "mass_transit_data_table_duplicate_ignore" AS ON INSERT TO "mass_transit_data"
            WHERE EXISTS(SELECT 1 FROM mass_transit_data
                        WHERE (vehicle_ref, time)=(NEW.vehicle_ref, NEW.time))
            DO INSTEAD NOTHING;
            '''))

    global global_statistics_table
    global_statistics_table = Table('global_statistics', metadata,
                              Column('id', Integer, primary_key=True),
                              Column('time', TIMESTAMP, nullable=False), #Only the date portion of time is used. TIMESTAMP datatype used for consistency.
                              Column('average_co2_usage', Float), #Daily co2 usage
                              Column('past_week_certificates_number', Integer, nullable=False),
                              Column('total_distance', Float, nullable=False), #Daily amount of distance
                              Index('idx_global_statistics_time', 'time'),)
    global_statistics_table.create(checkfirst=True)


    metadata.create_all(checkfirst=True)

    return db, store




# Helper Functions:

def get_rating(user_id, start_date, end_date):
    query = '''SELECT * FROM travelled_distances
               WHERE time >= :start_date
               AND time < :end_date
               AND user_id = :user_id;
               '''
    distance_rows = db.engine.execute(text(query), start_date=start_date, end_date=end_date, user_id=user_id)
    rows = distance_rows.fetchall()
    ranking = 0
    if len(rows) == 0:
        return EnergyRating(user_id), ranking

    rating = EnergyRating(user_id)
    for row in rows:
        rating.add_travelled_distances_row(row)
        ranking = row["ranking"]
    rating.calculate_rating()
    return rating, ranking


def get_svg(user_id):
    query = text('SELECT max(time) FROM travelled_distances')
    max_time = db.engine.execute(query).scalar()
    if max_time is None:
        max_time = datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

    end_time = max_time + timedelta(days=1)
    start_time = end_time - timedelta(days=7)
    rating, ranking = get_rating(user_id, start_time, end_time)

    query = text('''
        SELECT max(ranking) FROM travelled_distances WHERE time = :max_time''')
    max_ranking = db.engine.execute(query, max_time=max_time).scalar() or 0

    return svg_generation.generate_energy_rating_svg(rating, start_time, end_time, ranking, max_ranking)


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


def get_distinct_device_ids(datetime_start, datetime_end):
    return db.engine.execute(text('''
        SELECT DISTINCT device_id
        FROM device_data
        WHERE time >= :date_start
        AND time < :date_end;
    '''), date_start=str(datetime_start), date_end=str(datetime_end))


def get_filtered_device_data_points(user_id, datetime_start, datetime_end):
    query = '''
        SELECT time,
            ST_AsGeoJSON(coordinate) AS geojson,
            activity,
            line_type,
            line_name
        FROM device_data_filtered
        WHERE user_id = :user_id
        AND time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''
    points =  db.engine.execute(text(query), user_id=user_id, time_start=datetime_start, time_end=datetime_end)
    return points

def data_points_by_user_id(user_id, datetime_start, datetime_end):
    query = '''
        SELECT device_id,
            ST_AsGeoJSON(coordinate) AS geojson,
            activity_1, activity_1_conf,
            activity_2, activity_2_conf,
            activity_3, activity_3_conf,
            waypoint_id,
            time
        FROM device_data
        WHERE device_id IN (SELECT id FROM devices
                                 WHERE user_id = :user_id)
        AND time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''
    points =  db.engine.execute(text(query), user_id=user_id, time_start=datetime_start, time_end=datetime_end)
    return points

def data_points_snapping(device_id, datetime_start, datetime_end):
    qstart = '''
        SELECT id,
            ST_AsGeoJSON(coordinate) AS geojson,
            accuracy,
            activity_1, activity_1_conf,
            activity_2, activity_2_conf,
            activity_3, activity_3_conf,
            waypoint_id,
            time
        FROM device_data
    '''
    if device_id == 0:
        qstring = qstart + '''
        WHERE time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''
        points = db.engine.execute(text(qstring), time_start=datetime_start, time_end=datetime_end)
    else:
        qstring = qstart + '''
        WHERE device_id = :device_id
        AND time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''
        points =  db.engine.execute(text(qstring), device_id=device_id, time_start=datetime_start, time_end=datetime_end)
    return points

def data_points_filtered(user_id, datetime_start, datetime_end):
    qstring = '''
        SELECT id,
            ST_AsGeoJSON(coordinate) AS geojson,
            time,
            activity,
            line_type,
            line_name
        FROM device_data_filtered
        WHERE user_id = :user_id
        AND time >= :time_start
        AND time < :time_end
        ORDER BY time ASC
    '''
    points =  db.engine.execute(text(qstring), user_id=user_id, time_start=datetime_start, time_end=datetime_end)
    return points

def get_mass_transit_points(device_data_sample):
    # Get all mass transit points near a device data sample with timestamps close to each other.
    min_time = device_data_sample["time"] - datetime.timedelta(seconds=MAX_MASS_TRANSIT_TIME_DIFFERENCE)
    max_time = device_data_sample["time"] + datetime.timedelta(seconds=MAX_MASS_TRANSIT_TIME_DIFFERENCE)
    current_location = json.loads(device_data_sample["geojson"])["coordinates"]
    query = """SELECT line_type,
                      line_name,
                      vehicle_ref,
                      ST_AsGeoJSON(coordinate) AS geojson
               FROM mass_transit_data
               WHERE time >= :min_time
               AND time < :max_time
               AND ST_DWithin(coordinate, ST_Point(:longitude,:latitude), :MAX_MATCH_DISTANCE)"""

    mass_transit_points = db.engine.execute(text(query),
                                     min_time=min_time,
                                     max_time=max_time,
                                     longitude=current_location[0],
                                     latitude=current_location[1],
                                     MAX_MATCH_DISTANCE=MAX_MASS_TRANSIT_DISTANCE_DIFFERENCE)
    return mass_transit_points

def verify_user_id(user_id):
    if user_id is None or user_id == '':
        print 'empty user_id'
        abort(403)
    try:
        query = select([users_table.c.id]) \
            .where(users_table.c.user_id==user_id)
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
        query = select([devices_table.c.id]) \
            .where(devices_table.c.token==token)
        row = db.engine.execute(query).first()

        if not row:
            abort(403)
        return row[0]
    except DataError as e:
        print 'Exception: ' + e.message
        abort(403)


def update_last_activity(devices_table_id):
    update = devices_table.update() \
        .values({'last_activity': datetime.datetime.now()}) \
        .where(devices_table_id==devices_table_id)
    db.engine.execute(update)


def get_users_table_id_for_device(device_id, installation_id):
    try:
        query = select([devices_table.c.user_id]) \
            .where(devices_table.c.device_id==device_id) \
            .where(devices_table.c.installation_id==installation_id)
        row = db.engine.execute(query).first()
        if not row:
            return -1
        return int(row[0])
    except DataError as e:
        print 'Exception: ' + e.message

    return -1


def get_device_table_id(device_id, installation_id):
    try:
        query = select([devices_table.c.id]) \
            .where(devices_table.c.device_id==device_id) \
            .where(devices_table.c.installation_id==installation_id)
        row = db.engine.execute(query).first()
        if not row:
            return -1
        return int(row[0])
    except DataError as e:
        print 'Exception: ' + e.message

    return -1


def get_device_table_id_for_session(session_token):
    try:
        query = select([devices_table.c.id]) \
            .where(devices_table.c.token==session_token)
        row = db.engine.execute(query).first()
        if not row:
            return -1
        return int(row[0])
    except DataError:
        # invalid session token
        return -1


def get_user_id_from_device_id(device_id):
    try:
        query = select([devices_table.c.user_id]) \
            .where(devices_table.c.id==device_id)
        row = db.engine.execute(query).first()
        if not row:
            return None
        return int(row[0])
    except DataError as e:
        print 'Exception: ' + e.message

    return -1

def get_users_table_id(user_id):
    """
    :param user_id: user_id (hash value)
    :return: users.id (PK, Integer)
    """
    try:
        query = select([users_table.c.id]) \
            .where(users_table.c.user_id==user_id)
        row = db.engine.execute(query).first()
        if not row:
            return -1
        return int(row[0])
    except DataError as e:
        print 'Exception: ' + e.message

    return -1


def get_session_token_for_device(devices_table_id):
    if devices_table_id is None or devices_table_id < 0:
        return None
    try:
        # Curiously, using [devices_table.c.token] yields str, not UUID type
        query = select([column('token')]) \
            .where(devices_table.c.id==devices_table_id)
        row = db.engine.execute(query).first()
        if not row:
            return None
        return row[0]
    except DataError as e:
        print 'Exception: ' + e.message

    return None

def users_table_insert(user_id, refresh_token, access_token):
    stmt = users_table.insert({'google_refresh_token': refresh_token,
                               'google_server_access_token': access_token,
                               'user_id': user_id})
    db.engine.execute(stmt)
    return get_users_table_id(user_id)

def users_table_update(users_table_id, refresh_token, access_token):
    stmt = users_table.update() \
        .values({'google_refresh_token': refresh_token,
                 'google_server_access_token': access_token}) \
        .where(users_table.c.id==users_table_id)

    db.engine.execute(stmt)

def devices_table_insert(users_table_id, device_id, installation_id, device_model, session_token):
    device_insertion = devices_table.insert(
            {'user_id': users_table_id,
             'device_id': device_id,
             'installation_id': installation_id,
             'device_model': device_model,
             'token': session_token})
    db.engine.execute(device_insertion)

def device_data_table_insert(batch):
    db.engine.execute(device_data_table.insert(batch))

def device_data_filtered_table_insert(batch):
    db.engine.execute(device_data_filtered_table.insert(batch))

def db_engine_execute(query):
    return db.engine.execute(query)

