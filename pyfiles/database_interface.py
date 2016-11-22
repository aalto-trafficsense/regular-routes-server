#!/usr/bin/env python

import datetime
from datetime import timedelta

import geoalchemy2 as ga2
from flask import abort
from flask.ext.sqlalchemy import SQLAlchemy

from sqlalchemy import (
    BigInteger, Column, Enum, Float, ForeignKey, Index, Integer, String, Table,
    UniqueConstraint)

from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP, UUID
from sqlalchemy.exc import DataError, ProgrammingError

from sqlalchemy.sql import (
    and_, between, column, exists, func, or_, select, text)

import svg_generation

from pyfiles.energy_rating import EnergyRating
from pyfiles.config_helper import get_config

from constants import *


# from simplekv.memory import DictStore
from simplekv.db.sql import SQLAlchemyStore
from flask_kvsession import KVSessionExtension

import logging
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


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

# Declare the db and tables global, so they can be referenced from outside the init_db procedure
db = None
users_table = None
devices_table = None
device_data_table = None
device_data_filtered_table = None
legs_table = None
modes_table = None
leg_modes_view = None
leg_ends_table = None
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
                          Column('messaging_token', String, unique=True),
                          Column('last_activity', TIMESTAMP, nullable=False, default=func.current_timestamp(),
                                 server_default=func.current_timestamp()),
                          UniqueConstraint(
                              'user_id', 'device_id', 'installation_id',
                              name='uix_user_id_device_id_installation_id'))

    # Sample psql command to add the new 'messaging token' to an existing devices table:
    # ALTER TABLE devices ADD COLUMN messaging_token varchar UNIQUE ;

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
    # Deprecated by legs, below.
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

    # User leg ends are clustered into shared places
    global places_table
    places_table = Table('places', metadata,
        Column('id', Integer, primary_key=True),
        Column('coordinate', ga2.Geography('point', 4326, spatial_index=True)),
        Column('label', String),
        Column('label_coordinate', # coords when label was fetched
            ga2.Geography('point', 4326, spatial_index=True)))

    # Clustered leg ends
    global leg_ends_table
    leg_ends_table = Table('leg_ends', metadata,
        Column('id', Integer, primary_key=True),
        Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
        Column('coordinate', ga2.Geography('point', 4326, spatial_index=True)),
        Column('place', Integer, ForeignKey('places.id')))

    # Combined activity including mass transit submodes
    mode_enum = Enum(
        *(activity_types + mass_transit_types),
        name="mode_enum",
        metadata=metadata)

    # Decided activity and detected mass transit line in trace time ranges
    global legs_table
    legs_table = Table('legs', metadata,
        Column('id', Integer, primary_key=True),
        Column('device_id', Integer, ForeignKey('devices.id'), nullable=False),
        Column('user_id', Integer, ForeignKey('users.id')),
        Column('time_start', TIMESTAMP, nullable=False),
        Column('time_end', TIMESTAMP, nullable=False),
        Column('coordinate_start',
            ga2.Geography('point', 4326, spatial_index=False)),
        Column('coordinate_end',
            ga2.Geography('point', 4326, spatial_index=False)),
        Column('activity', activity_type_enum),
        Column('cluster_start', Integer, ForeignKey('leg_ends.id'), index=True),
        Column('cluster_end', Integer, ForeignKey('leg_ends.id'), index=True),

        # useful near beginning of time
        Index('idx_legs_user_id_time_start_time_end',
            'user_id', 'time_start', 'time_end'),
        # useful near end of time
        Index('idx_legs_user_id_time_end_time_start',
            'user_id', 'time_end', 'time_start'))
        # for something useful in the middle, query and gist index on tsrange

    # Transit mode and line names migrated from FILTERED data, detected from
    # LIVE vehicle locations or journey PLANNER, or provided by USER
    global modes_table
    modes_table = Table('modes', metadata,
        Column('id', Integer, primary_key=True),
        Column('leg',
            Integer,
            ForeignKey(legs_table.c.id, ondelete="CASCADE"),
            nullable=False),
        Column('source',
            Enum("FILTERED", "LIVE", "PLANNER", "USER",
                name="mode_source_enum"),
            nullable=False),
        Column('mode', mode_enum, nullable=False),
        Column('line', String),
        UniqueConstraint('leg', 'source'))

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
                              Column('coordinate', ga2.Geography('point', 4326), nullable=False),
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

    # Public transport service alerts
    global hsl_alerts_table
    hsl_alerts_table = Table('hsl_alerts', metadata,
                              Column('id', Integer, primary_key=True),
                              Column('alert_id', Integer, nullable=False),
                              Column('alert_start', TIMESTAMP, nullable=False),
                              Column('trip_start', TIMESTAMP, nullable=True),
                              Column('alert_end', TIMESTAMP, nullable=False),
                              Column('line_type', mass_transit_type_enum, nullable=False),
                              Column('line_name', String, nullable=False),
                              Column('direction', Integer),
                              Column('effect', Integer),
                              Column('fi_description', String),
                              Column('sv_description', String),
                              Column('en_description', String))

    if not hsl_alerts_table.exists():
        hsl_alerts_table.create(checkfirst=True)

    # Weather forecasts
    global weather_forecast_table
    weather_forecast_table = Table('weather_forecast', metadata,
        Column('id', Integer, primary_key=True),
        Column('time_retrieved', TIMESTAMP(timezone=True), nullable=False, default=func.current_timestamp(),
                                    server_default=func.current_timestamp()),
        Column('time_forecast', TIMESTAMP(timezone=True), nullable=False),
        Column('temperature', Float, nullable=False),
        Column('windspeed_ms', Float, nullable=False),
        Column('total_cloud_cover', Float, nullable=False),
        Column('precipitation_1h', Float, nullable=False))


    if not weather_forecast_table.exists():
        weather_forecast_table.create(checkfirst=True)


    # Weather observations
    global weather_observations_table
    weather_observations_table = Table('weather_observations', metadata,
        Column('id', Integer, primary_key=True),
        Column('time_retrieved', TIMESTAMP(timezone=True), nullable=False, default=func.current_timestamp(),
                                    server_default=func.current_timestamp()),
        Column('time_observed', TIMESTAMP(timezone=True), nullable=False),
        Column('temperature', Float, nullable=False),
        Column('windspeed_ms', Float, nullable=False),
        Column('precipitation_1h', Float, nullable=False))


    if not weather_observations_table.exists():
        weather_observations_table.create(checkfirst=True)


    # Traffic disorders
    global traffic_disorders_table
    traffic_disorders_table = Table('traffic_disorders', metadata,
        Column('id', Integer, primary_key=True),
        Column('time_retrieved', TIMESTAMP(timezone=True), nullable=False, default=func.current_timestamp(),
                                    server_default=func.current_timestamp()),
        Column('record_creation_time', TIMESTAMP(timezone=True), nullable=True),
        Column('disorder_id', String, nullable=False),
        Column('start_time', TIMESTAMP(timezone=True), nullable=True),
        Column('end_time', TIMESTAMP(timezone=True), nullable=True),
        Column('coordinate', ga2.Geography('point', 4326, spatial_index=False)),
        Column('waypoint_id', BigInteger),
        Column('fi_description', String, nullable=True),
        Column('sv_description', String, nullable=True),
        Column('en_description', String, nullable=True))


    if not traffic_disorders_table.exists():
        traffic_disorders_table.create(checkfirst=True)

    # Sample psql command to add the new 'coordinate' and 'waypoint_id' columns to an existing traffic_disorders table:
    # ALTER TABLE traffic_disorders ADD COLUMN coordinate geography(Point,4326) ;
    # ALTER TABLE traffic_disorders ADD COLUMN waypoint_id bigint ;



    # Public transport alerts matching to specific trip legs
    global pubtrans_legs_alerts_table
    pubtrans_legs_alerts_table = Table('pubtrans_legs_alerts', metadata,
                                Column('id', Integer, primary_key=True),
                                Column('time', TIMESTAMP(timezone=True), nullable=False,
                                     default=func.current_timestamp(),
                                     server_default=func.current_timestamp()),
                                Column('legs_table_id', Integer, ForeignKey('legs.id'), nullable=False),
                                Column('alert_table_id', Integer, ForeignKey('hsl_alerts.id'), nullable=False))

    if not pubtrans_legs_alerts_table.exists():
        pubtrans_legs_alerts_table.create(checkfirst=True)


    # Traffic disorders matching to specific device_data points
    global traffic_legs_disorders_table
    traffic_legs_disorders_table = Table('traffic_legs_disorders', metadata,
                                         Column('id', Integer, primary_key=True),
                                         Column('time', TIMESTAMP(timezone=True), nullable=False,
                                            default=func.current_timestamp(),
                                            server_default=func.current_timestamp()),
                                         Column('legs_table_id', Integer, ForeignKey('legs.id', ondelete="SET NULL")),
                                         Column('disorders_table_id', Integer, ForeignKey('traffic_disorders.id'), nullable=False))

    if not traffic_legs_disorders_table.exists():
        traffic_legs_disorders_table.create(checkfirst=True)


    # Alerts delivered
    global device_alerts_table
    device_alerts_table = Table('device_alerts', metadata,
        Column('id', Integer, primary_key=True),
        Column('time', TIMESTAMP(timezone=True), nullable=False,
                       default=func.current_timestamp(),
                       server_default=func.current_timestamp()),
        Column('device_id', Integer, ForeignKey('devices.id'), nullable=False),
        Column('messaging_token', String),
        Column('alert_end', TIMESTAMP, nullable=False),
        Column('alert_type', String),
        Column('coordinate',
            ga2.Geography('point', 4326, spatial_index=False)),
        Column('fi_text', String),
        Column('fi_uri', String),
        Column('en_text', String),
        Column('en_uri', String),
        Column('info', String))

    if not device_alerts_table.exists():
        device_alerts_table.create(checkfirst=True)

    # Commands to convert device_alerts table from the earlier version
    # ALTER TABLE device_alerts DROP COLUMN firebase_id ;
    # ALTER TABLE device_alerts ADD COLUMN messaging_token VARCHAR ;


    global global_statistics_table
    global_statistics_table = Table('global_statistics', metadata,
                              Column('id', Integer, primary_key=True),
                              Column('time', TIMESTAMP, nullable=False), #Only the date portion of time is used. TIMESTAMP datatype used for consistency.
                              Column('average_co2_usage', Float), #Daily co2 usage
                              Column('past_week_certificates_number', Integer, nullable=False),
                              Column('total_distance', Float, nullable=False), #Daily amount of distance
                              Index('idx_global_statistics_time', 'time'),)
    global_statistics_table.create(checkfirst=True)

    # log activity from both mobile and web clients
    # Note: Web client doesn't have a device_id: Using the latest one for the user
    global client_log_table
    client_log_table = Table('client_log', metadata,
        Column('id', Integer, primary_key=True),
        Column('device_id', Integer, ForeignKey('devices.id'), nullable=False),
        Column('user_id', Integer, ForeignKey('users.id')),
        Column('time', TIMESTAMP(timezone=True), nullable=False, default=func.current_timestamp(),
                                    server_default=func.current_timestamp()),
        Column('function',
            Enum("MOBILE-REGISTER", "MOBILE-AUTHENTICATE", "MOBILE-PATH",
                 "MOBILE-DESTINATIONS", "MOBILE-DEST-HISTORY", "MOBILE-CERTIFICATE",
                 "MOBILE-SHARE-CERTIFICATE", "MOBILE-PATH-EDIT", "MOBILE-FCM-TOKEN",
                 "WEB-CONNECT", "WEB-PATH", "WEB-CERTIFICATE", "WEB-TRIP-COMPARISON", "WEB-DEST-HISTORY",
                 "CANCEL-PARTICIPATION",
                 name="client_function_enum")),
        Column('info', String),
        Index('idx_client_log_time', 'time'))

    # Sample line to add new enum values to client_function_enum:
    # ALTER TYPE client_function_enum ADD VALUE 'MOBILE-FCM-TOKEN' ;

    if not client_log_table.exists():
        client_log_table.create(checkfirst=True)

    metadata.create_all(checkfirst=True)

    # Combined view of legs with migrated/detected/user modes and lines
    create = text("""

CREATE OR REPLACE VIEW leg_modes AS SELECT
    l.id,
    l.device_id,
    l.user_id,
    l.time_start,
    l.time_end,
    l.coordinate_start,
    l.coordinate_end,
    l.activity activity_device, -- rename in favor of user-corrected activity
    l.cluster_start,
    l.cluster_end,
    mu.mode mode_user, mu.line line_user,
    ml.mode mode_live, ml.line line_live,
    mp.mode mode_planner, mp.line line_planner,
    mf.mode mode_filtered, mf.line line_filtered,

    -- combined transit mode
    coalesce(mu.mode, ml.mode, mp.mode, mf.mode, activity::text::mode_enum)
        AS mode,

    -- strict activity including user correction, no mass transit
    CASE WHEN mu.mode IN :activity_types THEN mu.mode::text::activity_type_enum
        WHEN mu.mode IN :mass_transit_types THEN 'IN_VEHICLE'
        ELSE activity
        END activity,

    -- strict mass transit line_type, no activity
    coalesce(
        CASE WHEN mu.mode IN :mass_transit_types THEN mu.mode END,
        ml.mode,
        mp.mode,
        mf.mode) line_type,

    -- collateral coalesce of line on mode
    CASE WHEN mu.mode IS NOT NULL THEN mu.line
        WHEN ml.mode IS NOT NULL THEN ml.line
        WHEN mp.mode IS NOT NULL THEN mp.line
        WHEN mf.mode IS NOT NULL THEN mf.line END line_name

    FROM legs l
        LEFT JOIN modes mu ON mu.leg = l.id AND mu.source = 'USER'
        LEFT JOIN modes ml ON ml.leg = l.id AND ml.source = 'LIVE'
        LEFT JOIN modes mp ON mp.leg = l.id AND mp.source = 'PLANNER'
        LEFT JOIN modes mf ON mf.leg = l.id AND mf.source = 'FILTERED'
        """)

    params = {
        "activity_types": activity_types,
        "mass_transit_types": mass_transit_types}

    try:
        with db.engine.begin() as t:
            t.execute(create, **params)
    except ProgrammingError as e:
        # REPLACE VIEW is rather conservative; replace harder...
        with db.engine.begin() as t:
            t.execute(text("DROP VIEW leg_modes"))
            t.execute(create, **params)

    global leg_modes_view
    leg_modes_view = Table("leg_modes", metadata,
        Column("device_id", ForeignKey(devices_table.c.id)),
        Column("user_id", ForeignKey(users_table.c.id)),
        autoload=True)

    # Functions and triggers that maintain the leg_ends table
    with open("sql/legends.sql") as f, db.engine.begin() as t:
        # Server restarts can result in this being run concurrently, leading to
        # tuple concurrently updated and other racing on function and trigger
        # updates. Lock something vaguely relevant to serialize access.
        t.execute(text("lock leg_ends in access exclusive mode"))
        t.execute(text(f.read()), clustdist=2*DEST_RADIUS_MAX)

    # Functions and triggers that maintain the places table
    with open("sql/places.sql") as f, db.engine.begin() as t:
        t.execute(text("lock places in access exclusive mode"))
        t.execute(text(f.read()), clustdist=2*DEST_RADIUS_MAX)

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


def get_svg(user_id, firstday=None, lastday=None):
    if lastday is None:
        if firstday is None:
            # No params, end on last summarized user day, typically yesterday
            query = text('SELECT max(time) FROM travelled_distances')
            lastday = db.engine.execute(query).scalar()
        else:
            # Only start specified, show seven day window
            lastday = firstday + timedelta(days=6)
    if lastday is None:
        # No data or params, show seven day window up through yesterday
        lastday = datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    if firstday is None:
        # Default seven day window back
        firstday = lastday - timedelta(days=6)

    end_time = lastday + timedelta(days=1)
    rating, ranking = get_rating(user_id, firstday, end_time)

    # get_rating returns stored 7 day rating regardless of window length;
    # calculate true rating in window
    query = text("""
        WITH totals AS (SELECT
                user_id,
                sum(total_distance * average_co2) / sum(total_distance) co2
            FROM travelled_distances
            WHERE time >= :firstday AND time <= :lastday
            GROUP BY user_id ORDER BY co2),
        ranked AS (SELECT *, rank() OVER (order by co2) FROM totals),
        maxed AS (SELECT *, last_value(rank) OVER () FROM ranked)
        SELECT rank, last_value FROM maxed WHERE user_id = :user""")
    ranks = db.engine.execute(
        query, firstday=firstday, lastday=lastday, user=user_id).first()
    ranking, max_ranking = ranks or (0, 0)

    return svg_generation.generate_energy_rating_svg(rating, firstday, end_time, ranking, max_ranking)


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
    """Get trace with activity stabilized and mass transit detected, fusing
    legs and raw device data."""

    dd = db.metadata.tables["device_data"]
    legs = db.metadata.tables["leg_modes"]

    # Adjacent legs both cover their join point, but only if considered close
    # enough, so retrieving each point only once for the filtered data flavor
    # requires some finessing...
    legs = select(
        [   func.lag(legs.c.time_start) \
                .over(partition_by=legs.c.user_id, order_by=legs.c.time_start)\
                .label("prev_end"),
            legs.c.device_id,
            legs.c.time_start,
            legs.c.time_end,
            legs.c.activity,
            legs.c.line_type,
            legs.c.line_name],
        and_(
            legs.c.user_id == user_id,
            legs.c.activity != None,
            legs.c.time_start <= datetime_end,
            legs.c.time_end >= datetime_start)).alias("lagged")

    return db.engine.execute(select(
        [   func.ST_AsGeoJSON(dd.c.coordinate).label("geojson"),
            dd.c.time,
            legs.c.activity,
            legs.c.line_type,
            legs.c.line_name],
        and_(
            dd.c.time >= datetime_start,
            dd.c.time < datetime_end),
        legs.join(dd, and_(
            legs.c.device_id == dd.c.device_id,
            between(dd.c.time, legs.c.time_start, legs.c.time_end),
            or_(legs.c.prev_end == None, dd.c.time > legs.c.prev_end))),
        order_by=dd.c.time))


def get_filtered_device_data_points_OLD(user_id, datetime_start, datetime_end):
    """Get trace with activity stabilized and mass transit detected, from
    legacy device_data_filtered."""

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

def data_points_by_user_id_after(user_id, datetime_start, datetime_end):
    query = '''
        SELECT device_id,
            ST_AsGeoJSON(coordinate) AS geojson,
            activity_1, activity_1_conf,
            activity_2, activity_2_conf,
            activity_3, activity_3_conf,
            waypoint_id,
            time
        FROM device_data
        WHERE device_id IN (SELECT id FROM devices WHERE user_id = :user_id)
        AND time > :time_start
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


def get_waypoint_id_from_coordinate(coordinate):
    """Return the identifier of the waypoint closest to a given coordinate.
    :param: coordinate (geography(Point,4326))
    :return: waypoint_id (bigint)
    """
    try:
        row = db.engine.execute(text("""
          SELECT id
          FROM roads_waypoints
          JOIN waypoints
          ON waypoint_id = waypoints.id
          LEFT JOIN LATERAL (
            SELECT osm_id
            FROM roads
            WHERE ST_DWithin(roads.geo, :coordinate, 100)
            ORDER BY ST_Distance(roads.geo, :coordinate) ASC
            LIMIT 1
          ) AS road ON true
          WHERE road_id = road.osm_id
          ORDER BY ST_Distance(waypoints.geo, :coordinate) ASC
          LIMIT 1 ;"""), coordinate=coordinate).first()
        if not row:
            return None
        return int(row[0])
    except DataError as e:
        print 'Exception in get_waypoint_id_from_coordinate: ' + e.message
    return None


def match_mass_transit_legs(device, tstart, tend, activity):
    """Find mass transit matches already recorded in an existing leg, or None
    if leg matching start/end/activity."""

    legs = db.metadata.tables["legs"]
    modes = db.metadata.tables["modes"]

    where = and_(
        legs.c.device_id == device,
        legs.c.time_start == tstart,
        legs.c.time_end == tend,
        legs.c.activity == activity)

    if not db.engine.execute(select([exists().where(where)])).scalar():
        return None

    return db.engine.execute(select(
        [modes.c.source, modes.c.mode, modes.c.line],
        where,
        legs.join(modes))).fetchall()


def match_mass_transit_filtered(device, tstart, tend):
    """Find mass transit match from legacy filtered data. Returns None if
    no filtered data for this device beyond end of range; otherwise (line_type,
    line_name) pair."""

    # Find out if filtered data extends this far forward. Due to gaps, looking
    # at just the given range would lead to firing actual detectors for very
    # old data.
    if None is db.engine.execute(select(
            [1],
            and_(
                device_data_filtered_table.c.time >= tend,
                devices_table.c.id == device),
            device_data_filtered_table.join(
                devices_table,
                device_data_filtered_table.c.user_id==devices_table.c.user_id),
            ).limit(1)).scalar():
        return None

    # XXX Should rather ORDER BY (line_type, line_name), so the pair can't be
    # mismatched, but the result row record seems like a pain to unpack?
    return db.engine.execute(
        text("""
            SELECT
                mode() WITHIN GROUP (ORDER BY line_type) line_type,
                mode() WITHIN GROUP (ORDER BY line_name) line_name
            FROM device_data_filtered f JOIN devices d USING (user_id)
            WHERE d.id = :device AND time BETWEEN :tstart AND :tend"""),
        device=device, tstart=tstart, tend=tend).first()


def match_mass_transit_live(device, tstart, tend, tradius, dradius, nsamples):
    """Find mass transit vehicles near user during a trip leg.

    Arguments:
    device -- device_data.device_id
    tstart -- start timestamp of leg
    tend -- end timestamp of leg
    tradius -- slack allowed in seconds between device and vehicle data point
    dradius -- slack allowed in metres between device and vehicle data point
    nsamples -- match using given number of points at most

    Result columns:
    revsum -- each metre inside dradius counts toward the summed distance score
    hitrate -- fraction of times device and vehicle within dradius and tradius
    vehicle_ref, line_type, line_name -- as in mass_transit_data

    If no mass transit vehicle data exists prior to tstart, as would be the
    case when older data has been deleted, return None.
    """

    # Find out if mass_transit_data extends this far back.
    if None is db.engine.execute(select(
            [1], mass_transit_data_table.c.time <= tstart).limit(1)).scalar():
        return None

    return db.engine.execute(
        text("""

-- find the relevant device data points
WITH fulltrace AS (
    SELECT coordinate, id, time, row_number() OVER (ORDER BY time) rn
    FROM device_data
    WHERE device_id = :device AND time >= :tstart AND time <= :tend),

-- sample from full trace to limit quadratic matching; uses integer sampling
-- interval so will end up with between [n/2, n] samples
trace AS (
    SELECT *
    FROM fulltrace
    WHERE rn % (
        SELECT ceil((1 + max(rn)) / (1.0 + :nsamples)) FROM fulltrace) = 0),

-- rough bbox margin meters to degrees of lon at lat, so overshoots on latitude
m2lon AS (
    SELECT :dradius
        / cos(pi() * max(abs(ST_Y(coordinate::geometry))) / 180)
        / 110574 AS x
    FROM trace),

-- bounding box for mass transit data, expand not symmetric in m but that's ok
bbox AS (
    SELECT ST_Expand(ST_Extent(coordinate::geometry), (SELECT x from m2lon)) x
    FROM trace),

-- bound the mass transit data in time and space
boxed AS (
    SELECT coordinate, vehicle_ref, line_type, line_name, time
    FROM mass_transit_data
    WHERE time > timestamp :tstart - interval ':tradius seconds'
        AND time < timestamp :tend + interval ':tradius seconds'
        AND coordinate::geometry @ (SELECT x FROM bbox)),

-- Join device points with linestrings of the trace of each vehicle around that
-- time. The line_name changes randomly on some vehicles, pick most frequent.
linetraces AS (
    SELECT
        d.id,
        d.coordinate,
        m.vehicle_ref,
        ST_MakeLine(m.coordinate::geometry ORDER BY m.time) line_trace,
        mode() WITHIN GROUP (ORDER BY m.line_type) line_type,
        mode() WITHIN GROUP (ORDER BY m.line_name) line_name
    FROM boxed m JOIN trace d
    ON abs(extract(epoch from (m.time - d.time))) <= :tradius
    GROUP BY d.id, d.coordinate, m.vehicle_ref),

-- Score matches by the distance inward of the match radius.
nearest AS (
    SELECT
        id,
        vehicle_ref,
        line_type,
        line_name,
        :dradius - ST_Distance(line_trace, coordinate) revdist
    FROM linetraces
    WHERE ST_Distance(line_trace, coordinate) <= :dradius)

-- Sum scores and count matches over user location trace. Some vehicles'
-- line_name and other fields flip randomly, pick most frequent.
    SELECT
        sum(revdist) revsum,
        1.0 * count(*) / (SELECT count(*) FROM trace) hitrate,
        vehicle_ref,
        mode() WITHIN GROUP (ORDER BY line_type) line_type,
        mode() WITHIN GROUP (ORDER BY line_name) line_name
    FROM nearest
    GROUP BY vehicle_ref order by hitrate desc, revsum desc"""),

        device=device,
        tstart=tstart,
        tend=tend,
        tradius=tradius,
        dradius=dradius,
        nsamples=nsamples)


def hsl_alerts_insert(alerts):
    if alerts:
        db.engine.execute(hsl_alerts_table.insert(alerts))


def hsl_alerts_get_max():
    """
    :return: max_alert_id, max_alert_end (max int, max timestamp)
    """
    try:
        max_alert_id = None
        query = select([func.max(hsl_alerts_table.c.alert_id)])
        row = db.engine.execute(query).first()
        if row and row[0]:
            max_alert_id = int(row[0])
        max_alert_end = None
        query = select([func.max(hsl_alerts_table.c.alert_end)])
        row = db.engine.execute(query).first()
        if row and row[0]:
            max_alert_end = row[0]
        return max_alert_id, max_alert_end
    except DataError as e:
        print 'Exception in hsl_alerts_get_max: ' + e.message
    return -1, -1


def match_legs_traffic_disorder(selection, disorder):
    try:
        query = '''
          SELECT {0}
          FROM device_data, leg_modes legs
          WHERE
            device_data.time >= now()-(interval '1 month') AND
            ("time"(:start AT TIME ZONE 'Europe/Helsinki') - "time"(time)
                NOT BETWEEN interval '-24h' + :margin AND
                             interval '24h' - :margin OR
             "time"(:start AT TIME ZONE 'Europe/Helsinki') - "time"(time)
                BETWEEN - interval :margin AND interval :margin) AND
            legs.activity = 'IN_VEHICLE' AND
            legs.device_id = device_data.device_id AND
            device_data.time BETWEEN time_start AND time_end AND
            ST_Distance(device_data.coordinate, :coordinate) < 300;'''
        return db.engine.execute(
            text(query.format(selection)),
            margin="2 hours",
            start=disorder["start_time"],
            coordinate=disorder["coordinate"]).fetchall()
    except Exception as e:
        print "match_legs_traffic_disorder exception: ", e
        return None


def match_traffic_disorder(disorder):
    try:
        # Find matching legs
        legs_ids = match_legs_traffic_disorder("DISTINCT legs.id", disorder)
        if len(legs_ids) > 0:
            # Find the corresponding traffic_disorders table id
            traffic_disorders_query = select([traffic_disorders_table.c.id]) \
                .where(traffic_disorders_table.c.disorder_id == disorder["disorder_id"]) \
                .where(traffic_disorders_table.c.record_creation_time == disorder["record_creation_time"])
            traffic_disorders_response = db.engine.execute(traffic_disorders_query).first()
            for matched_leg in legs_ids:
                # Save the legs - alerts pairs to traffic_legs_disorders
                stmt = traffic_legs_disorders_table.insert({'legs_table_id': matched_leg["id"],
                                   'disorders_table_id': traffic_disorders_response["id"]})
                db.engine.execute(stmt)

            # Search again to find the distinct users to inform
            user_ids = match_legs_traffic_disorder("DISTINCT user_id", disorder)
            if len(user_ids) > 0:
                for user in user_ids:
                    # Find the latest eligible device id for this user
                    response = get_active_device_info_from_users_table_id(user["user_id"])
                    if response:    # This user has an eligible device
                        device_id = response["id"]
                        messaging_token = response["messaging_token"]
                        # Did we already send the same alert text to the same device today?
                        if len(todays_alert_text_matches(device_id, disorder["fi_description"])) < 1:
                            # No identical alert text found
                            # Parse digitraffic ISO8601 timestamp into a datetime without timezone
                            # (doesn't work if the server doesn't give a correct localzone)
                            # alert_end = parse(disorder["end_time"]).astimezone(get_localzone())
                            # This didn't have the expected effect: .replace(tzinfo=None) + ae_with_tz.tzinfo._offset
                            # Trust Digitraffic platform to take care of DST:
                            if disorder["end_time"] is None:
                                # No endtime specified - default 1 hour from now
                                alert_end = datetime.datetime.now() + datetime.timedelta(hours=1)
                            else:
                                # Strip into a timezone-naive datetime:
                                alert_end = datetime.datetime.strptime(disorder["end_time"][:19], "%Y-%m-%dT%H:%M:%S")
                            # Create an alert
                            device_alert = {'device_id': device_id,
                                            'messaging_token': messaging_token,
                                            'alert_end': alert_end,
                                            'alert_type': "DIGITRAFFIC",
                                            'coordinate': disorder["coordinate"],
                                            'fi_text': disorder["fi_description"],
                                            'fi_uri': None,
                                            'en_text': disorder["fi_description"],
                                            'en_uri': None,
                                            'info': disorder["disorder_id"]}
                            # Save the new alert to device_alerts
                            stmt = device_alerts_table.insert(device_alert)
                            db.engine.execute(stmt)
                            yield device_alert
    except Exception as e:
        print "match_traffic_disorder exception: ", e


def match_legs_pubtrans_alert(selection, alert):
    try:
        query = '''
          SELECT {0}
          FROM leg_modes
          WHERE
            time_start >= now()-(interval '1 month') AND
            ("time"(time_start) - "time"(:trip_start)
                NOT BETWEEN interval '-24h' + :margin AND
                             interval '24h' - :margin OR
             "time"(time_start) - "time"(:trip_start)
                BETWEEN - interval :margin and interval :margin) AND
            line_type = :line_type AND
            line_name = :line_name;'''
        return db.engine.execute(
            text(query.format(selection)),
            margin="2 hours",
            trip_start=alert["trip_start"],
            line_type=alert["line_type"],
            line_name=alert["line_name"]).fetchall()
    except Exception as e:
        print "match_legs_pubtrans_alert exception: ", e
        return None


def match_pubtrans_alert(alert):
    try:
        # Find matching legs
        legs_ids = match_legs_pubtrans_alert("id", alert)
        if len(legs_ids) > 0:
            for matched_leg in legs_ids:
                # Find the corresponding hsl_alerts table id
                hsl_alerts_query = select([hsl_alerts_table.c.id]) \
                    .where(hsl_alerts_table.c.alert_id==alert["alert_id"]) \
                    .where(hsl_alerts_table.c.line_name == alert["line_name"]) \
                    .where(hsl_alerts_table.c.direction == alert["direction"])
                hsl_alerts_response = db.engine.execute(hsl_alerts_query).first()
                # Save the legs - alerts pairs to pubtrans_legs_alerts
                stmt = pubtrans_legs_alerts_table.insert({'legs_table_id': matched_leg["id"],
                                   'alert_table_id': hsl_alerts_response["id"]})
                db.engine.execute(stmt)

            # Search again to find the distinct users to inform
            user_ids = match_legs_pubtrans_alert("DISTINCT user_id", alert)
            if len(user_ids) > 0:
                for user in user_ids:
                    # Find the latest eligible device id for this user
                    response = get_active_device_info_from_users_table_id(user["user_id"])
                    if response:    # This user has an eligible device
                        device_id = response["id"]
                        messaging_token = response["messaging_token"]
                        # Did we already send the same alert text to the same device today?
                        if len(todays_alert_text_matches(device_id, alert["fi_description"])) < 1:
                            # No identical alert text found
                            # Create an alert
                            # If no endtime specified, default 1 hour from now:
                            if alert["alert_end"] is None:
                                alert_end = datetime.datetime.now() + datetime.timedelta(hours=1)
                            else:
                                alert_end = alert["alert_end"]
                            device_alert = {'device_id': device_id,
                                         'messaging_token': messaging_token,
                                         'alert_end': alert_end,
                                         'alert_type': alert["line_type"],
                                         'fi_text': alert["fi_description"],
                                         'fi_uri': "https://www.reittiopas.fi/disruptions.php",
                                         'en_text': alert["en_description"],
                                         'en_uri': "https://www.reittiopas.fi/en/disruptions.php",
                                         'info': alert["line_type"] + ": " + alert["line_name"]}
                            # Save the new alert to device_alerts
                            stmt = device_alerts_table.insert(device_alert)
                            db.engine.execute(stmt)
                            yield device_alert
    except Exception as e:
        print "match_pubtrans_alert exception: ", e


# A test procedure to push all incoming alerts to a hardcoded client
# Check the device_id and TEST_MSG_TOKEN (in regularroutes.cfg) before running a test!
def match_pubtrans_alert_test(alert):
    try:
        device_id = 192
        messaging_token = get_config('TEST_MSG_TOKEN')
        # Did we already send the same alert text to the same device today?
        if len(todays_alert_text_matches(device_id, alert["fi_description"])) < 1:
            # No identical alert text found
            # Create an alert
            device_alert = {'device_id': device_id,
                            'messaging_token': messaging_token,
                            'alert_end': alert["alert_end"],
                            'alert_type': alert["line_type"],
                            'fi_text': alert["fi_description"],
                            'fi_uri': "https://www.reittiopas.fi/disruptions.php",
                            'en_text': alert["en_description"],
                            'en_uri': "https://www.reittiopas.fi/en/disruptions.php",
                            'info': alert["line_type"] + ": " + alert["line_name"]}
            yield device_alert
    except Exception as e:
        print "match_pubtrans_alert_test exception: ", e


def todays_alert_text_matches(devices_table_id, fi_text):
    """
    Find potential duplicates to
    :param devices_table_id, fi_text (integer, string)
    :return: devices_alerts.ids
    """
    try:
        return db.engine.execute(text("""
          SELECT id
          FROM device_alerts
          WHERE
            device_id = :devices_table_id AND
            time > date_trunc('day', now()) AND
            fi_text = :fi_text ;"""), devices_table_id=devices_table_id, fi_text=fi_text).fetchall()
    except DataError as e:
        print 'Exception in todays_alert_text_matches: ' + e.message
    return -1



def weather_forecast_insert(weather):
    if weather:
        db.engine.execute(weather_forecast_table.insert(weather))


def weather_observations_insert(weather):
    if weather:
        db.engine.execute(weather_observations_table.insert(weather))


def traffic_disorder_max_creation():
    """
    :return: max record creation time (timestamp with tz)
    """
    try:
        max_time = None
        query = select([func.max(traffic_disorders_table.c.record_creation_time)])
        row = db.engine.execute(query).first()
        if row and row[0]:
            max_time = row[0]
        return max_time
    except DataError as e:
        print 'Exception in traffic_disorder_max_creation: ' + e.message
    return -1


def traffic_disorder_insert(disorders):
    if disorders:
        db.engine.execute(traffic_disorders_table.insert(disorders))


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
        .where(devices_table.c.id == devices_table_id)
    db.engine.execute(update)


def get_device_table_id(user_id, device_id, installation_id):
    query = select([devices_table.c.id]) \
        .where(devices_table.c.user_id==user_id) \
        .where(devices_table.c.device_id==device_id) \
        .where(devices_table.c.installation_id==installation_id)
    return db.engine.execute(query).scalar()


def update_messaging_token(devices_table_id, new_msg_token):
    update = devices_table.update() \
        .values({'last_activity': datetime.datetime.now()}) \
        .values({'messaging_token': new_msg_token}) \
        .where(devices_table.c.id == devices_table_id)
    db.engine.execute(update)


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
    """
    :param devices.id (client number, integer)
    :return: users.id (integer)
    """
    try:
        query = select([devices_table.c.user_id]) \
            .where(devices_table.c.id==device_id)
        row = db.engine.execute(query).first()
        if not row:
            return None
        return int(row[0])
    except DataError as e:
        print 'Exception in get_user_id_from_device_id: ' + e.message
    return -1


def get_max_devices_table_id_from_users_table_id(users_table_id):
    """
    :param users_table_id (devices.user_id, integer)
    :return: devices.id (max, integer)
    """
    try:
        query = select([func.max(devices_table.c.id)]) \
            .where(devices_table.c.user_id==users_table_id)
        row = db.engine.execute(query).first()
        if not row:
            return None
        return int(row[0])
    except DataError as e:
        print 'Exception in get_max_devices_table_id_from_users_table_id: ' + e.message
    return -1


def get_active_device_info_from_users_table_id(users_table_id):
    """
    Get a known user's devices_table_id, which has the latest data upload (at least within a week),
    from coordinates around the Helsinki region and carries a messaging token
    :param users_table_id (devices.user_id, integer)
    :return: devices.id (integer)
    """
    # Ref: Sample query capturing the area from Inkoo to Askola (ST_MakeEnvelope(left, bottom, right, top, srid))
    # SELECT count(*) FROM device_data WHERE ST_Intersects(coordinate, ST_MakeEnvelope(23.85211, 59.88652, 25.73626, 60.55221, 4326)) ;
    try:
        row = db.engine.execute(text("""
          SELECT devices.id, devices.messaging_token
          FROM devices, device_data
          WHERE
            device_data.time > now()-(interval '1 week') AND
            devices.user_id = :users_table_id AND
            device_data.device_id = devices.id AND
            devices.messaging_token != '' AND
            ST_Intersects(device_data.coordinate, ST_MakeEnvelope(23.85211, 59.88652, 25.73626, 60.55221, 4326))
          ORDER BY device_data.time DESC
          LIMIT 1 ;"""), users_table_id=users_table_id).first()
        if not row:
            return None
        return row
    except DataError as e:
        print 'Exception in get_active_devices_table_id_from_users_table_id: ' + e.message
    return None


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
             'token': session_token}) \
        .returning(devices_table.c.id)
    return db.engine.execute(device_insertion).scalar()


def device_data_table_insert(batch):
    db.engine.execute(device_data_table.insert(batch))

def device_data_filtered_table_insert(batch):
    db.engine.execute(device_data_filtered_table.insert(batch))

def client_log_table_insert(device_id, user_id, client_function, info):
    client_log_insertion = client_log_table.insert(
            {'device_id': device_id,
             'user_id': user_id,
             'function': client_function,
             'info': info})
    db.engine.execute(client_log_insertion)


def db_engine_execute(query):
    return db.engine.execute(query)


