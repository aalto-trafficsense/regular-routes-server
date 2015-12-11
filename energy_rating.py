from constants import *
import datetime
import json
import math

class EnergyRating:

    def __init__(self, user_id, in_vehicle_distance=0, in_mass_transit_A_distance=0, in_mass_transit_B_distance=0, in_mass_transit_C_distance=0, on_bicycle_distance=0, running_distance=0, walking_distance=0, date=datetime.datetime.now()):
        self.user_id = user_id
        self.date = date.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
        self.in_vehicle_distance = in_vehicle_distance
        self.on_bicycle_distance = on_bicycle_distance
        self.running_distance = running_distance
        self.walking_distance = walking_distance
        self.in_mass_transit_A_distance = in_mass_transit_A_distance
        self.in_mass_transit_B_distance = in_mass_transit_B_distance
        self.in_mass_transit_C_distance = in_mass_transit_C_distance
        self.calculate_rating()


    def add_in_vehicle_distance(self, distance):
        self.in_vehicle_distance += distance

    def add_on_bicycle_distance(self, distance):
        self.on_bicycle_distance += distance

    def add_running_distance(self, distance):
        self.running_distance += distance

    def add_walking_distance(self, distance):
        self.walking_distance += distance

    def add_in_mass_transit_A_distance(self, distance):
        self.in_mass_transit_A_distance += distance

    def add_in_mass_transit_B_distance(self, distance):
        self.in_mass_transit_B_distance += distance

    def add_in_mass_transit_C_distance(self, distance):
        self.in_mass_transit_C_distance += distance

    def is_empty(self):
        return self.total_distance == 0

    def calculate_rating(self):
        self.total_distance = self.in_vehicle_distance + \
                              self.on_bicycle_distance + \
                              self.walking_distance + \
                              self.running_distance + \
                              self.in_mass_transit_A_distance + \
                              self.in_mass_transit_B_distance + \
                              self.in_mass_transit_C_distance
        if self.total_distance == 0:
            self.running_percentage = 0
            self.walking_percentage = 0
            self.in_vehicle_percentage = 0
            self.on_bicycle_percentage = 0
            self.in_mass_transit_A_percentage = 0
            self.in_mass_transit_B_percentage = 0
            self.in_mass_transit_C_percentage = 0
        else:
            self.running_percentage = self.running_distance / self.total_distance
            self.walking_percentage = self.walking_distance / self.total_distance
            self.in_vehicle_percentage = self.in_vehicle_distance / self.total_distance
            self.on_bicycle_percentage = self.on_bicycle_distance / self.total_distance
            self.in_mass_transit_A_percentage = self.in_mass_transit_A_distance / self.total_distance
            self.in_mass_transit_B_percentage = self.in_mass_transit_B_distance / self.total_distance
            self.in_mass_transit_C_percentage = self.in_mass_transit_C_distance / self.total_distance

        self.average_co2 = self.on_bicycle_percentage * ON_BICYCLE_CO2 + \
                           self.walking_percentage * WALKING_CO2 + \
                           self.running_percentage * RUNNING_CO2 + \
                           self.in_vehicle_percentage * IN_VEHICLE_CO2 + \
                           self.in_mass_transit_A_percentage * MASS_TRANSIT_A_CO2 + \
                           self.in_mass_transit_B_percentage * MASS_TRANSIT_B_CO2 + \
                           self.in_mass_transit_C_percentage * MASS_TRANSIT_C_CO2

        self.total_co2 = self.average_co2 * self.total_distance

        self.final_rating = (self.average_co2 - ON_BICYCLE_CO2) / IN_VEHICLE_CO2 # value becomes 0-1 with 1 being the worst.

    def get_data_dict(self):
        return {
            'user_id':self.user_id,
            'time':self.date,
            'cycling':self.on_bicycle_distance,
            'walking':self.walking_distance,
            'running':self.running_distance,
            'mass_transit_a':self.in_mass_transit_A_distance,
            'mass_transit_b':self.in_mass_transit_B_distance,
            'mass_transit_c':self.in_mass_transit_C_distance,
            'car':self.in_vehicle_distance,
            'total_distance':self.total_distance,
            'average_co2':self.average_co2
        }

    def add_travelled_distances_row(self, travelled_distances_row):
        if travelled_distances_row["cycling"] is not None:
            self.on_bicycle_distance += travelled_distances_row["cycling"]
        if travelled_distances_row["walking"] is not None:
            self.walking_distance += travelled_distances_row["walking"]
        if travelled_distances_row["running"] is not None:
            self.running_distance += travelled_distances_row["running"]
        if travelled_distances_row["mass_transit_a"] is not None:
            self.in_mass_transit_A_distance += travelled_distances_row["mass_transit_a"]
        if travelled_distances_row["mass_transit_b"] is not None:
            self.in_mass_transit_B_distance += travelled_distances_row["mass_transit_b"]
        if travelled_distances_row["mass_transit_c"] is not None:
            self.in_mass_transit_C_distance += travelled_distances_row["mass_transit_c"]
        if travelled_distances_row["car"] is not None:
            self.in_vehicle_distance += travelled_distances_row["car"]


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

