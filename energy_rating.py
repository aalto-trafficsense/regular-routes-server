from constants import *
import json
import math

class EnergyRating:


    def __init__(self, device_data_rows):
        self.analyze_rows(device_data_rows)

    def analyze_rows(self, device_data_rows):
        rows = device_data_rows.fetchall()
        bad_activities = ("UNKNOWN", "TILTING", "STILL")
        if len(rows) == 0:
            self.set_values(0, 0, 0, 0, 0, 0, 0)
            return
        in_vehicle_distance = 0
        on_bicycle_distance = 0
        walking_distance = 0
        running_distance = 0
        previous_time = rows[0]["time"]
        current_activity = rows[0]["activity_1"]
        previous_location = json.loads(rows[0]["geojson"])["coordinates"]

        for row in rows[1:]:
            if row["activity_1"] in bad_activities and current_activity == "UNKNOWN":
                continue
            current_activity = row["activity_1"]
            current_time = row["time"]

            if (current_time - previous_time).total_seconds() > 300:
                previous_time = current_time
                continue

            current_location = json.loads(row["geojson"])["coordinates"]

            # from: http://stackoverflow.com/questions/1253499/simple-calculations-for-working-with-lat-lon-km-distance
            # The approximate conversions are:
            # Latitude: 1 deg = 110.574 km
            # Longitude: 1 deg = 111.320*cos(latitude) km

            xdiff = (previous_location[0] - current_location[0]) * 110.320 * math.cos((current_location[1] / 360) * math.pi)
            ydiff = (previous_location[1] - current_location[1]) * 110.574

            distance = (xdiff * xdiff + ydiff * ydiff)**0.5

            previous_location = current_location

            if current_activity == "IN_VEHICLE":
                in_vehicle_distance += distance
            elif current_activity == "ON_BICYCLE":
                on_bicycle_distance += distance
            elif current_activity == "RUNNING":
                running_distance += distance
            elif current_activity == "WALKING":
                walking_distance += distance

        self.set_values(in_vehicle_distance, 0, 0, 0, on_bicycle_distance, running_distance, walking_distance)

    def set_values(self, in_vehicle_distance, in_mass_transit_A_distance, in_mass_transit_B_distance, in_mass_transit_C_distance, on_bicycle_distance, running_distance, walking_distance):
        self.in_vehicle_distance = in_vehicle_distance
        self.on_bicycle_distance = on_bicycle_distance
        self.running_distance = running_distance
        self.walking_distance = walking_distance
        self.in_mass_transit_A_distance = in_mass_transit_A_distance
        self.in_mass_transit_B_distance = in_mass_transit_B_distance
        self.in_mass_transit_C_distance = in_mass_transit_C_distance
        self.total_distance = in_vehicle_distance + on_bicycle_distance + walking_distance + running_distance + in_mass_transit_A_distance + in_mass_transit_B_distance + in_mass_transit_C_distance
        if self.total_distance == 0:
            self.running_percentage = 0
            self.walking_percentage = 0
            self.in_vehicle_percentage = 0
            self.on_bicycle_percentage = 0
            self.in_mass_transit_A_percentage = 0
            self.in_mass_transit_B_percentage = 0
            self.in_mass_transit_C_percentage = 0
        else:
            self.running_percentage = running_distance / self.total_distance
            self.walking_percentage = walking_distance / self.total_distance
            self.in_vehicle_percentage = in_vehicle_distance / self.total_distance
            self.on_bicycle_percentage = on_bicycle_distance / self.total_distance
            self.in_mass_transit_A_percentage = in_mass_transit_A_distance / self.total_distance
            self.in_mass_transit_B_percentage = in_mass_transit_B_distance / self.total_distance
            self.in_mass_transit_C_percentage = in_mass_transit_C_distance / self.total_distance

        self.average_co2 = self.on_bicycle_percentage * ON_BICYCLE_CO2 + \
                           self.walking_percentage * WALKING_CO2 + \
                           self.running_percentage * RUNNING_CO2 + \
                           self.in_vehicle_percentage * IN_VEHICLE_CO2 + \
                           self.in_mass_transit_A_percentage * MASS_TRANSIT_A_CO2 + \
                           self.in_mass_transit_B_percentage * MASS_TRANSIT_B_CO2 + \
                           self.in_mass_transit_C_percentage * MASS_TRANSIT_C_CO2

        self.total_co2 = self.average_co2 * self.total_distance

        self.final_rating = (self.average_co2 - ON_BICYCLE_CO2) / IN_VEHICLE_CO2 # value becomes 0-1 with 1 being the worst.


    def __str__(self):
        return_string = "\
        Distances:<br>\
        In vehicle: {in_vehicle_distance}<br>\
        On bicycle: {on_bicycle_distance}<br>\
        Running: {running_distance}<br>\
        Walking: {walking_distance}<br>\
        Total: {total_distance}<br>\
        <br>\
        Percentages:<br>\
        In vehicle: {in_vehicle_percentage}<br>\
        On bicycle: {on_bicycle_percentage}<br>\
        Running: {running_percentage}<br>\
        Walking: {walking_percentage}<br>\
        <br>\
        Average CO2 emission (g): {average_co2}<br>\
        Total CO2 emission (g): {total_co2}<br>".format(in_vehicle_distance=self.in_vehicle_distance,
                                                        on_bicycle_distance=self.on_bicycle_distance,
                                                        running_distance=self.running_distance,
                                                        walking_distance=self.walking_distance,
                                                        total_distance=self.total_distance,
                                                        in_vehicle_percentage=self.in_vehicle_percentage,
                                                        on_bicycle_percentage=self.on_bicycle_percentage,
                                                        running_percentage=self.running_percentage,
                                                        walking_percentage=self.walking_percentage,
                                                        average_co2=self.average_co2,
                                                        total_co2=self.total_co2)
        return return_string