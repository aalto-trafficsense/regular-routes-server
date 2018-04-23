# -*- coding: utf-8 -*-
#
# Retrieve information from external information sources to the TrafficSense service
#
# Created by: mikko.rinne@aalto.fi 14.11.2016
#
import json
# import urllib.request, urllib.error, urllib.parse
import requests
import datetime
from datetime import timedelta
import re
import xml.etree.ElementTree as ET

import dateutil.parser
import requests
from google.transit import gtfs_realtime_pb2
from owslib.wfs import WebFeatureService

from pyfiles.common_helpers import interpret_jore
from pyfiles.database_interface import hsl_alerts_get_max, traffic_disorder_max_creation, get_waypoint_id_from_coordinate
from pyfiles.constants import gtfs_route_types, gtfs_effects
from pyfiles.config_helper import get_config

import logging
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

debug_input = False
save_alert_sample = False


def hsl_alert_request():
    debug_input_filename = "pyfiles/HSLAlertSample.txt"
    alert_feed = gtfs_realtime_pb2.FeedMessage()
    if debug_input:
        with open(debug_input_filename) as data_file:
            sample_str = file.read(data_file)
        alert_feed.ParseFromString(sample_str)
    else:
        url = 'http://api.digitransit.fi/realtime/service-alerts/v1/'
#         response = urllib.request.urlopen(url, timeout=50)
#         alert_feed.ParseFromString(response.read())
        response = requests.get(url, timeout=50)
        alert_feed.ParseFromString(response)
        if save_alert_sample:
            with open(debug_input_filename, "w") as data_file:
                data_file.write(alert_feed.SerializeToString())

    new_alerts = []
    max_alert_id, max_alert_end = hsl_alerts_get_max()
    # print 'max_alert_id: ', max_alert_id
    # print 'max_alert_end: ', max_alert_end

    def hsl_alert_row(inf_ent):
        try:
            agency_id = inf_ent.agency_id
            line_name, line_type = interpret_jore(inf_ent.route_id)
            gtfs_route_type = inf_ent.route_type
            trip = inf_ent.trip
            trip_time = trip.start_time
            td = trip.start_date
            trip_start = td[:4] + '-' + td[4:6] + '-' + td[6:8] + ' ' + trip_time
            if len(trip_start) < 16:
                trip_start = str(alert_start)  # for multi-line alerts no trip start time is included
            direction_id = trip.direction_id
        except:
            log.exception("information_services / retrieve_hsl_service_alerts: Error in alert parsing.")

        # print
        # print "ID:" + str(alert_id) + " Alert start: " + str(alert_start) + " Trip start: " + trip_start + " Alert End: " + str(alert_end)
        # print "Agency: " + agency_id + " JoRe line type: " + line_type + " GTFS route type: " + gtfs_route_types[gtfs_route_type] + " Line name: " + line_name + " Direction: " + str(direction_id)
        # print "Effect: " + gtfs_effects[effect]

        return {
            'alert_id': alert_id,
            'alert_start': alert_start,
            'trip_start': trip_start,
            'alert_end': alert_end,
            'line_type': line_type,
            'line_name': line_name,
            'direction': direction_id,
            'effect': effect,
            'fi_description': fi_description,
            'sv_description': sv_description,
            'en_description': en_description
        }

    for alert in alert_feed.entity:
        # print alert
        try:
            alert_id = alert.id
            trip_update_prefix = 'trip_update:'
            if not alert_id.startswith(trip_update_prefix):
                alert_id = int(alert_id)
                duplicate_candidate = False
                duplicate_alert = False
                if max_alert_id:
                    if alert_id <= max_alert_id: duplicate_candidate = True
                alert = alert.alert
                effect = alert.effect
                act_p = alert.active_period[0]
                alert_start = toDateTime(act_p.start)
                alert_end = toDateTime(act_p.end)
                if max_alert_end:
                    if (alert_end <= max_alert_end) and duplicate_candidate:
                        # print 'Skipping alert: ', alert_id
                        duplicate_alert = True
                # duplicate_alert = False  # TODO: Comment out this line - it is for debugging purposes!!
                if not duplicate_alert:
                    fi_description = "Ei viestiä"
                    sv_description = "Ingen meddelande"
                    en_description = "No message"
                    for language_variant in alert.description_text.translation:
                        language = language_variant.language
                        description = language_variant.text
                        if language == 'fi':
                            fi_description = description
                        elif language == 'en':
                            en_description = description
                        elif language == 'sv':
                            sv_description = description
                    # print "Language: " + language + " Message: " + description
                    # print

                    for informed_entity in alert.informed_entity:
                        new_row = hsl_alert_row(informed_entity)
                        if new_row:
                            new_alerts.append(new_row)

        except Exception as e:
            log.exception("Failed to handle alert: %s" % alert)
    return new_alerts


# MJR: Started this, but as of 2016-11-15 this API is returning the same contents as the
# realtime service alerts API, and HSL confirmed that they have no other content available
# at this time. Therefore not finalising.
def retrieve_hsl_disruptions():
    debug_input_filename = "HSLDisruptionSample.txt"
    json_data = '{}'
    if debug_input:
        with open(debug_input_filename) as data_file:
            json_data = json.load(data_file)
    else:
        url = 'https://api.digitransit.fi/routing/v1/routers/hsl/index/graphql'
        querystr = "{alerts{alertDescriptionText}}"
        json_data = graphql_request(url, querystr)
        if save_alert_sample:
            with open(debug_input_filename, "w") as data_file:
                json.dump(json_data, data_file)
    print(json.dumps(json_data))


def fmi_forecast_request():
    # Get next 24H
    utc_hour = get_utc_now_current_hour()
    starttime = utc_hour + timedelta(hours=1)
    endtime = utc_hour + timedelta(hours=24)
    return fmi_request('fmi::forecast::hirlam::surface::point::simple', wfs_utc_string(starttime), wfs_utc_string(endtime),
                       weather_forecast_params, 'time_forecast')

# Dictionary listing for forecast queries the meteorological parameters to fetch and our table column names they map to
weather_forecast_params = {'Temperature': 'temperature',
                           'WindSpeedMS': 'windspeed_ms',
                           'TotalCloudCover': 'total_cloud_cover',
                           'Precipitation1h': 'precipitation_1h'}

# Available parameters & explanations:
#
# Temperature:
# WindSpeedMS: Wind averaged over 10 minutes
# TotalCloudCover:
# Precipitation1h:
#
# GeopHeight
# Pressure
# Humidity
# WindDirection
# WindSpeedMS
# WindUMS
# WindVMS
# MaximumWind
# WindGust
# DewPoint
# TotalCloudCover
# WeatherSymbol3
# LowCloudCover
# MediumCloudCover
# HighCloudCover
# PrecipitationAmount
# RadiationGlobalAccumulation
# RadiationLWAccumulation
# RadiationNetSurfaceLWAccumulation
# RadiationNetSurfaceSWAccumulation
# RadiationDiffuseAccumulation


def fmi_observations_request():
    # Get last 24H
    utc_hour = get_utc_now_current_hour()
    starttime = utc_hour + timedelta(hours=-25)
    endtime = utc_hour + timedelta(hours=-2)
    return fmi_request('fmi::observations::weather::simple', wfs_utc_string(starttime), wfs_utc_string(endtime),
                       weather_observation_params, 'time_observed')

# Dictionary listing for historical observation queries the meteorological parameters to fetch and our table column names they map to
weather_observation_params = {'t2m': 'temperature',
                              'ws_10min': 'windspeed_ms',
#                               'TotalCloudCover': 'total_cloud_cover', # this is 0-8, forecast 0-100?!?!
                              'r_1h': 'precipitation_1h'}


# Available observation parameters & explanations:
#
# t2m: temperature on the height of 2 meters
# ws_10min: 10-minute windspeed average
# wg_10min: highest-velocity wind-gust during 10 minutes
# wd_10min: wind-direction calculated over 10 minutes
# rh: relative humidity
# td: dew-point (kastepiste)
# r_1h: rain buildup during one hour
# ri_10min: rain intensity during 10min
# snow_aws: Snow depth, automatic station
# p_sea: Air pressure at sea level
# vis: visibility
# wawa: The code of the current weather

def get_utc_now_current_hour():
    return datetime.datetime.utcnow().replace(minute=0,second=0,microsecond=0)


def wfs_utc_string(dt):
    return dt.isoformat('T')+'Z'


def fmi_request(query_id, start_time, end_time, weather_params, time_row_label):
    debug_input_filename = "pyfiles/FMIObservationSample.xml"
    ns = {'wfs': 'http://www.opengis.net/wfs/2.0',
          'BsWfs': 'http://xml.fmi.fi/schema/wfs/2.0',
          'gml': 'http://www.opengis.net/gml/3.2'}
    xml_root = ""
    if debug_input:
        xml_root = ET.parse(debug_input_filename).getroot()
    else:
        fmi_wfs = WebFeatureService(url='http://data.fmi.fi/fmi-apikey/' + get_config('FMI_API_KEY') + '/wfs',
                                    version='2.0.0')
        keys = ",".join(list(weather_params.keys()))
        query_params = {'place': 'helsinki',
                       'starttime': start_time,
                       'endtime': end_time,
                       'timestep': 60,  # minutes
                       'parameters': keys}
        try:
            feature_read = fmi_wfs.getfeature(storedQueryID=query_id, storedQueryParams=query_params).read()
            xml_root = ET.fromstring(feature_read)
            if save_alert_sample:
                with open(debug_input_filename, "w") as data_file:
                    data_file.write(bytes(feature_read))
        except Exception as e:
            print("fmi_forecast_request exception: ", e)
    time_retrieved = xml_root.get('timeStamp')
    response_table = []
    first_name = None
    nan_counter = 0
    row_build = {'time_retrieved': time_retrieved}
    for member in xml_root.iterfind('wfs:member', ns):
        for item in list(member):
            name_elem = item.find('BsWfs:ParameterName', ns).text
            if not first_name: first_name = name_elem
            elif name_elem == first_name:
                response_table.append(row_build)
                row_build = {'time_retrieved': time_retrieved}
            row_build[time_row_label] = item.find('BsWfs:Time', ns).text
            value_elem = item.find('BsWfs:ParameterValue', ns).text
            if value_elem == 'NaN':
                value_elem = 0
                nan_counter += 1
            if name_elem in weather_params:
                row_build[weather_params[name_elem]] = value_elem
    if len(row_build) > 2: response_table.append(row_build)
    if nan_counter > 24: response_table = None  # Sometimes all values return as 'NaN' all the way from FMI. Useless result - rather reload later
    return response_table


def graphql_request(urlstr, querystr):
    json_data = '{}'
    try:
        header = {'Content-Type': 'application/graphql'}
        response = requests.post(urlstr, headers = header, data = querystr, verify=True)
        json_data = response.json()
        response.close()
    except requests.exceptions.ConnectionError as e:
        print("graphql_request / requests.exceptions.ConnectionError: ", e)
    except Exception as e:
        print("graphql_request / exception: ", e)
    return json_data


ns = {'sju': 'http://tie.digitraffic.fi/schemas',
      'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
      'pp': 'http://datex2.eu/schema/2/2_0',
      'xsi': 'http://www.w3.org/2001/XMLSchema-instance'}


def traffic_disorder_request():
    debug_input_filename = "TrafficAlertSample.xml"
    xml_root = ""
    if debug_input:
        xml_root = ET.parse(debug_input_filename).getroot()
    else:
        try:
            url = 'https://tie.digitraffic.fi/api/v1/data/traffic-disorders-datex2'
#            response_read = urllib.request.urlopen(url, timeout=50).read()
            response_read = requests.get(url, timeout=50)
            xml_root = ET.fromstring(response_read)
            if save_alert_sample:
                with open(debug_input_filename, "w") as data_file:
                    data_file.write(bytes(response_read))
        except Exception as e:
            print("Traffic disorder fetch exception: ", e)

    new_disorders = []
    max_creation_time = traffic_disorder_max_creation()
    # print "Max creation time: ", max_creation_time

    def traffic_disorder_row(record):
        end_time = None
        fi_description = None
        sv_description = None
        en_description = None
        coordinate = None
        waypoint = None
        try:
            disorder_id = record.get('id')
            record_creation_time = dateutil.parser.parse(record.find('pp:situationRecordCreationTime', ns).text)
            # print "Record creation time: ", record_creation_time
            if max_creation_time:
                if record_creation_time <= max_creation_time: return None
            validity = record.find('pp:validity', ns)
            validity_time_spec = validity.find('pp:validityTimeSpecification', ns)
            start_time = validity_time_spec.find('pp:overallStartTime', ns).text
            try:
                end_time = validity_time_spec.find('pp:overallEndTime', ns).text
            except:
                "Common issue - end time frequently not included."
            # print disorder_id, start_time, end_time
            comments = record.find('pp:generalPublicComment', ns).find('pp:comment', ns).find('pp:values', ns).iterfind(
                'pp:value', ns)
            for language_variant in comments:
                language = language_variant.get('lang')
                description = language_variant.text
                if language == 'fi':
                    # Only take the ones under "tieliikennekeskus Helsinki"
#                    if description.find("Helsinki") < 0: return None

                    # Strip the constant contact information
                    footerstarts = [
                        "http://liikennetilanne.liikennevirasto.fiLiikennevir",
                        "Liikenne- ja kelitiedot verkossa: http://liikennetil"]
                    for footer in footerstarts:
                        i = description.find(footer)
                        if i != -1:
                            description = description[:i]

                    # Compact whitespace in description
                    description = re.sub("[\\n ]+", " ", description)

                    fi_description = description
                elif language == 'en':
                    en_description = description
                elif language == 'sv':
                    sv_description = description
            # Try to get some coordinates
            try:
                group_of_locations = record.find('pp:groupOfLocations', ns)
                if group_of_locations is not None:
                    alert_c_sub = group_of_locations.find('pp:alertCLinear', ns)
                    if alert_c_sub is None:
                        alert_c_sub = group_of_locations.find('pp:alertCPoint', ns)
                    if alert_c_sub is not None:
                        # Method currently not checked
                        # alert_c_method = alert_c_sub.get('{http://www.w3.org/2001/XMLSchema-instance}type')
                        table_version = alert_c_sub.find('pp:alertCLocationTableVersion', ns).text
                        point = alert_c_sub.find('pp:alertCMethod4PrimaryPointLocation', ns)
                        if point is None:
                            point = alert_c_sub.find('pp:alertCMethod2PrimaryPointLocation', ns)
                        if point is None:
                            point = alert_c_sub.find('pp:alertCMethod3PrimaryPointLocation', ns)
                        if point is None:
                            point = alert_c_sub.find('pp:alertCMethod1PrimaryPointLocation', ns)
                        if point is not None:
                            loc = point.find('pp:alertCLocation', ns).find('pp:specificLocation', ns).text
                            lat, lng = getAlertC(table_version, loc)
                            if lat is not None:
                                coordinate = 'POINT(%f %f)' % (float(lng), float(lat))
                                waypoint = get_waypoint_id_from_coordinate(coordinate)


                                # Note: Calculating based on multiple points seems to displace the final point from the
                # main road, where the disorder took place. For matching with waypoints, it appears
                # better to just take the primary point independent of what other points were given.
                # Therefore commenting out the stuff below...
                # if alert_c_method == 'AlertCMethod4Linear':
                #     point1 = alert_c_sub.find('pp:alertCMethod4PrimaryPointLocation', ns)
                #     loc1, offset1 = getAlertCLoc(point1)
                #     lat1, lng1 = getAlertC(table_version, loc1)
                #     point2 = alert_c_sub.find('pp:alertCMethod4SecondaryPointLocation', ns)
                #     loc2, offset2 = getAlertCLoc(point2)
                #     lat2, lng2 = getAlertC(table_version, loc2)
                #     ratio = (offset1 + 0.0) / (offset1 + offset2)
                #     lat = lat1 + (lat2 - lat1) * ratio
                #     lng = lng1 + (lng2 - lng1) * ratio
                # elif alert_c_method == 'AlertCMethod2Linear':
                #     # Haven't seen any spec for this --> averaging the two points
                #     point1 = alert_c_sub.find('pp:alertCMethod4PrimaryPointLocation', ns)
                #     loc1 = point1.find('pp:alertCLocation', ns).find('pp:specificLocation', ns).text
                #     lat1, lng1 = getAlertC(table_version, loc1)
                #     point2 = alert_c_sub.find('pp:alertCMethod4SecondaryPointLocation', ns)
                #     loc2 = point2.find('pp:alertCLocation', ns).find('pp:specificLocation', ns).text
                #     lat2, lng2 = getAlertC(table_version, loc2)
                #     lat = (lat1 + lat2) / 2
                #     lng = (lng1 + lng2) / 2

            except:
                "Common issue - no coordinates in traffic alert."
            return {
                'record_creation_time': record_creation_time,
                'disorder_id': disorder_id,
                'start_time': start_time,
                'end_time': end_time,
                'coordinate': coordinate,
                'waypoint_id': waypoint,
                'fi_description': fi_description,
                'sv_description': sv_description,
                'en_description': en_description
            }
        except Exception as e:
            print("Traffic disorder row build exception: ", e)
            return None

    try:
        for disorder in xml_root:
            row = traffic_disorder_row(disorder.find('sju:d2LogicalModel', ns)
                                       .find('pp:payloadPublication', ns)
                                       .find('pp:situation', ns)
                                       .find('pp:situationRecord', ns))
            if row: new_disorders.append(row)
    except Exception as e:
        print("Traffic disorder loop exception: ", e)
        new_disorders = []
    return new_disorders


def getAlertCLoc(point):
    loc = point.find('pp:alertCLocation', ns).find('pp:specificLocation', ns).text
    offset = point.find('pp:offsetDistance', ns).find('pp:offsetDistance', ns).text
    return loc, int(offset)


def toDateTime(hsl_sec):
    return datetime.datetime.fromtimestamp(hsl_sec)


def getAlertC(version, location):
    # https://tie-test.digitraffic.fi/api/v1/metadata/locations/1596?version=1.11.30
    try:
        coords = None
        response = requests.get('http://tie-test.digitraffic.fi/api/v1/metadata/locations/' + location + '?version=' + version)
        # json_response = json.loads(response.text)
        json_response = response.json()
        response.close()
        features = json_response.get('features')
        if features:
            # for feature in features:
            geom = features[0].get('geometry')
            if geom:
                coords = geom.get('coordinates')
        return coords[1], coords[0]
    except Exception as e:
        print("getAlertC was unable to fetch coordinates: ", e)
        return None, None

        # hsl_alert_request()
# print fmi_observations_request()
# print fmi_forecast_request()
# print traffic_disorder_request()

# coordinates = getAlertC("1.11.30", "1596")
# if coordinates:
#     print "Lng: " + str(coordinates[0]) + " Lat: " + str(coordinates[1])

