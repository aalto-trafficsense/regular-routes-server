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
MAX_DIFFERENT_DEVICE_TIME_DIFFERENCE = 20

#Required number of different activities for triggering an activity change.
CONSECUTIVE_DIFFERENCE_LIMIT = 10


#Regexes for the HSL api
jore_ferry_regex = re.compile("^1019")
jore_subway_regex = re.compile("^1300")
jore_rail_regex = re.compile("^300")
jore_tram_regex = re.compile("^10(0|10)")
jore_bus_regex = re.compile("^(1|2|4)...")

jore_tram_replace_regex = re.compile("^.0*")
jore_bus_replace_regex = re.compile("^.0*")