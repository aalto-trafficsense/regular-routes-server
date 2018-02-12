import re

#CO2 emissions per km:
ON_BICYCLE_CO2 = 21
WALKING_CO2 = 33
RUNNING_CO2 = 47
MASS_TRANSIT_A_CO2 = 80
MASS_TRANSIT_B_CO2 = 90
MASS_TRANSIT_C_CO2 = 100
IN_VEHICLE_CO2 = 150


#Maximum time limit between two device data points in seconds before they're considered to be two separate trips
MAX_POINT_TIME_DIFFERENCE = 300

# Maximum time allowed between points before splitting a stop segment.
STOP_BREAK_INTERVAL = 60*60*24

#If two devices for the same user id have produced activity data within this time limit (in seconds), the one with the larger device_id is dropped.
MAX_DIFFERENT_DEVICE_TIME_DIFFERENCE = 40

# The maximum amount of time difference (in seconds) between a device data sample and a mass transit data item
# the mass transit matching algorithm will allow. Minimum value = 15 because mass transit data is sampled every 30 seconds.
MAX_MASS_TRANSIT_TIME_DIFFERENCE = 60

# The maximum distance (in metres) between a device data sample and a mass transit data item
# the mass transit matching algorithm will allow. Note that trains can be long, yet each provides only one set of coordinates.
MAX_MASS_TRANSIT_DISTANCE_DIFFERENCE = 100

# The number of samples that device data filtering will use when matching the IN_VEHICLE sequence with mass transit data.
# Must be equal or smaller than CONSECUTIVE_DIFFERENCE_LIMIT
NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES = 40

# Maximum number of misses in mass transit sampling. Ensures that one bad sample doesn't ruin the matching.
MAXIMUM_MASS_TRANSIT_MISSES = 10

# Required number of different activities for triggering an activity change in device_data_filtering.
# Must be equal or greater than MASS_TRANSIT_MATCH_SAMPLES
CONSECUTIVE_DIFFERENCE_LIMIT = 6

# Size in seconds of window for deciding activity, should match
# 2 * CONSECUTIVE_DIFFERENCE_LIMIT * client reporting interval.
ACTIVITY_WIN = 120

# Repeat false accurate location points are typically clustered close together;
# this threshold distance is used to collect them together for discarding.
# Increasing it may lose useful points in valid stops.
BAD_LOCATION_RADIUS = 10

# Alert user of traffic disorders if within this distance of event
ALERT_RADIUS = 100000

# Minimum and maximum distance from given coordinates to show destinations to
# client api
INCLUDE_DESTINATIONS_BETWEEN = 200, 200000

# Default maximum number of destinations to emit in client api
DESTINATIONS_LIMIT = 5

# Links to transit disruption pages
DISRUPTION_URI_EN = "https://www.hsl.fi/en/news"
DISRUPTION_URI_FI = "https://www.hsl.fi/ajankohtaista"

# Minimum duration for activity location flavor of stop for trip splitting
TRIP_STOP_DURATION = "30 min"

#Regexes for the HSL api
jore_ferry_regex = re.compile("^1019")
jore_subway_regex = re.compile("^1300")
jore_rail_regex = re.compile("^300")
jore_tram_regex = re.compile("^10(0|10)")
jore_bus_regex = re.compile("^(1|2|4|5|6|9)...")

jore_tram_replace_regex = re.compile("^.0*")
jore_bus_replace_regex = re.compile("^.0*")


# Maximum distance (m) and minimum duration (s) for detecting a stopover.
DEST_DURATION_MIN = 300
DEST_RADIUS_MAX = 100

# Maximum time interval (ms) between incoming location point and activity detection
# MAX_LOCATION_ACTIVITY_INTERVAL_MS = 60000 # +- one minute
MAX_LOCATION_ACTIVITY_INTERVAL_MS = 180000 # testing +- three minutes hour

# Activity mapping from integer values (RNTravelSenseService / ActivityData.h)
int_activities = {
    0: 'UNKNOWN', # NOT_SET not in the current enum
    1: 'IN_VEHICLE',
    2: 'ON_BICYCLE',
    3: 'ON_FOOT',
    4: 'RUNNING',
    5: 'STILL',
    6: 'TILTING',
    7: 'UNKNOWN',
    8: 'WALKING' }

# GTFS Route Types
gtfs_route_types = {    0: 'Tram, Light Rail, Streetcar',
                        1: 'Subway, Metro',
                        2: 'Rail',
                        3: 'Bus',
                        4: 'Ferry',
                        5: 'Cable Car',
                        6: 'Gondola, Suspended cable car',
                        7: 'Funicular',
                    100: 'Railway Service',
                    101: 'High Speed Rail Service',
                    102: 'Long Distance Trains',
                    103: 'Inter Regional Rail Service',
                    104: 'Car Transport Rail Service',
                    105: 'Sleeper Rail Service',
                    106: 'Regional Rail Service',
                    107: 'Tourist Railway Service',
                    108: 'Rail Shuttle (Within Complex)',
                    109: 'Suburban Railway',
                    110: 'Replacement Rail Service',
                    111: 'Special Rail Service',
                    112: 'Lorry Transport Rail Service',
                    113: 'All Rail Services',
                    114: 'Cross-Country Rail Service',
                    115: 'Vehicle Transport Rail Service',
                    116: 'Rack and Pinion Railway',
                    117: 'Additional Rail Service',
                    200: 'Coach Service',
                    201: 'International Coach Service',
                    202: 'National Coach Service',
                    203: 'Shuttle Coach Service',
                    204: 'Regional Coach Service',
                    205: 'Special Coach Service',
                    206: 'Sightseeing Coach Service',
                    207: 'Tourist Coach Service',
                    208: 'Commuter Coach Service',
                    209: 'All Coach Services',
                    300: 'Suburban Railway Service',
                    400: 'Urban Railway Service',
                    401: 'Metro Service',
                    402: 'Underground Service',
                    403: 'Urban Railway Service',
                    404: 'All Urban Railway Services',
                    405: 'Monorail',
                    500: 'Metro Service',
                    600: 'Underground Service',
                    700: 'Bus Service',
                    701: 'Regional Bus Service',
                    702: 'Express Bus Service',
                    703: 'Stopping Bus Service',
                    704: 'Local Bus Service',
                    705: 'Night Bus Service',
                    706: 'Post Bus Service',
                    707: 'Special Needs Bus',
                    708: 'Mobility Bus Service',
                    709: 'Mobility Bus for Registered Disabled',
                    710: 'Sightseeing Bus',
                    711: 'Shuttle Bus',
                    712: 'School Bus',
                    713: 'School and Public Service Bus',
                    714: 'Rail Replacement Bus Service',
                    715: 'Demand and Response Bus Service',
                    716: 'All Bus Services',
                    800: 'Trolleybus Service',
                    900: 'Tram Service',
                    901: 'City Tram Service',
                    902: 'Local Tram Service',
                    903: 'Regional Tram Service',
                    904: 'Sightseeing Tram Service',
                    905: 'Shuttle Tram Service',
                    906: 'All Tram Services',
                    1000: 'Water Transport Service',
                    1001: 'International Car Ferry Service',
                    1002: 'National Car Ferry Service',
                    1003: 'Regional Car Ferry Service',
                    1004: 'Local Car Ferry Service',
                    1005: 'International Passenger Ferry Service',
                    1006: 'National Passenger Ferry Service',
                    1007: 'Regional Passenger Ferry Service',
                    1008: 'Local Passenger Ferry Service',
                    1009: 'Post Boat Service',
                    1010: 'Train Ferry Service',
                    1011: 'Road-Link Ferry Service',
                    1012: 'Airport-Link Ferry Service',
                    1013: 'Car High-Speed Ferry Service',
                    1014: 'Passenger High-Speed Ferry Service',
                    1015: 'Sightseeing Boat Service',
                    1016: 'School Boat',
                    1017: 'Cable-Drawn Boat Service',
                    1018: 'River Bus Service',
                    1019: 'Scheduled Ferry Service',
                    1020: 'Shuttle Ferry Service',
                    1021: 'All Water Transport Services',
                    1100: 'Air Service',
                    1101: 'International Air Service',
                    1102: 'Domestic Air Service',
                    1103: 'Intercontinental Air Service',
                    1104: 'Domestic Scheduled Air Service',
                    1105: 'Shuttle Air Service',
                    1106: 'Intercontinental Charter Air Service',
                    1107: 'International Charter Air Service',
                    1108: 'Round-Trip Charter Air Service',
                    1109: 'Sightseeing Air Service',
                    1110: 'Helicopter Air Service',
                    1111: 'Domestic Charter Air Service',
                    1112: 'Schengen-Area Air Service',
                    1113: 'Airship Service',
                    1114: 'All Air Services',
                    1200: 'Ferry Service',
                    1300: 'Telecabin Service',
                    1301: 'Telecabin Service',
                    1302: 'Cable Car Service',
                    1303: 'Elevator Service',
                    1304: 'Chair Lift Service',
                    1305: 'Drag Lift Service',
                    1306: 'Small Telecabin Service',
                    1307: 'All Telecabin Services',
                    1400: 'Funicular Service',
                    1401: 'Funicular Service',
                    1402: 'All Funicular Service',
                    1500: 'Taxi Service',
                    1501: 'Communal Taxi Service',
                    1502: 'Water Taxi Service',
                    1503: 'Rail Taxi Service',
                    1504: 'Bike Taxi Service',
                    1505: 'Licensed Taxi Service',
                    1506: 'Private Hire Service Vehicle',
                    1507: 'All Taxi Services',
                    1600: 'Self Drive',
                    1601: 'Hire Car',
                    1602: 'Hire Van',
                    1603: 'Hire Motorbike',
                    1604: 'Hire Cycle',
                    1700: 'Miscellaneous Service',
                    1701: 'Cable Car',
                    1702: 'Horse-drawn Carriage' }

# GTFS effects
gtfs_effects = {
    1: 'NO_SERVICE',
    2: 'REDUCED_SERVICE',
    3: 'SIGNIFICANT_DELAYS',
    4: 'DETOUR',
    5: 'ADDITIONAL_SERVICE',
    6: 'MODIFIED_SERVICE',
    7: 'OTHER_EFFECT',
    8: 'UNKNOWN_EFFECT',
    9: 'STOP_MOVED' }
