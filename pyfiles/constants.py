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
NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES = 4

# Maximum number of misses in mass transit sampling. Ensures that one bad sample doesn't ruin the matching.
MAXIMUM_MASS_TRANSIT_MISSES = 1

# Required number of different activities for triggering an activity change in device_data_filtering.
# Must be equal or greater than MASS_TRANSIT_MATCH_SAMPLES
CONSECUTIVE_DIFFERENCE_LIMIT = 6

# Size in seconds of window for deciding activity.
ACTIVITY_WIN = 60


#Regexes for the HSL api
jore_ferry_regex = re.compile("^1019")
jore_subway_regex = re.compile("^1300")
jore_rail_regex = re.compile("^300")
jore_tram_regex = re.compile("^10(0|10)")
jore_bus_regex = re.compile("^(1|2|4)...")

jore_tram_replace_regex = re.compile("^.0*")
jore_bus_replace_regex = re.compile("^.0*")