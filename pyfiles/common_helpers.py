import math

def get_distance_between_coordinates(coord1, coord2):
    # from: http://stackoverflow.com/questions/1253499/simple-calculations-for-working-with-lat-lon-km-distance
    # The approximate conversions are:
    # Latitude: 1 deg = 110.574 km
    # Longitude: 1 deg = 111.320*cos(latitude) km

    x_diff = (coord1[0] - coord2[0]) * 110.320 * math.cos((coord2[1] / 360) * math.pi)
    y_diff = (coord1[1] - coord2[1]) * 110.574

    distance = (x_diff * x_diff + y_diff * y_diff)**0.5
    return distance
