import json

from pyfiles.constants import *
from pyfiles.common_helpers import *
from database_interface import get_mass_transit_points
from pyfiles.database_interface import init_db, device_data_filtered_table_insert


class DeviceDataFilterer:

    def __init__(self):
        self.previous_activity_1 = "NOT_SET"
        self.previous_activity_2 = "NOT_SET"
        self.previous_activity_3 = "NOT_SET"
        self.previous_activity_1_conf = 0
        self.previous_activity_2_conf = 0
        self.previous_activity_3_conf = 0

    def _flush_device_data_queue(self, device_data_queue, activity, user_id):
        if len(device_data_queue) == 0:
            return
        filtered_device_data = []

        line_type, line_name = self._match_mass_transit_line(activity, device_data_queue)

        for device_data_row in device_data_queue:
            current_location = json.loads(device_data_row["geojson"])["coordinates"]
            filtered_device_data.append({"activity" : activity,
                                         "user_id" : user_id,
                                         'coordinate': 'POINT(%f %f)' % (float(current_location[0]), float(current_location[1])),
                                         "time" : device_data_row["time"],
                                         "waypoint_id" : device_data_row["waypoint_id"],
                                         "line_type": line_type,
                                         "line_name": line_name})

        device_data_filtered_table_insert(filtered_device_data)

    def _match_mass_transit_line(self, activity, device_data_queue):
        if activity != "IN_VEHICLE":
            return None, None


        vehicle_data = {} # Contains the line name and line type of a vehicle id
        match_counts = {} # Contains the number of matches per vehicle id
        total_distances = {} # Contains the total distance from device_data samples to mass transit samples
        collected_matches = [] # Contains lists of vehicle matches. One list per sampling point.
        final_matches = []

        # Get NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES samples from device_data_queue and try to match those samples
        # with mass transit data.
        sampling_factor = len(device_data_queue) / NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES
        for i in range(NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES):
            #print i # print to get some responses of the progress.
            sample = device_data_queue[i * sampling_factor]
            mass_transit_points = get_mass_transit_points(sample)
            new_vehicles_and_distances = {} # Dictionary: vehicle_id, distance to device_data sample
            matches_and_distances = []
            sample_location = json.loads(sample["geojson"])["coordinates"]
            for point in mass_transit_points:
                vehicle_location = json.loads(point["geojson"])["coordinates"]
                # Get all distinct nearby vehicle ids and their distances from the device data sample and store their data
                distance = get_distance_between_coordinates(sample_location, vehicle_location)
                
                # Because there can be multiple matches for a single mass transport vehicle,
                # we pick the closest one.
                if point["vehicle_ref"] in new_vehicles_and_distances:
                    if new_vehicles_and_distances[point["vehicle_ref"]] > distance:
                        new_vehicles_and_distances[point["vehicle_ref"]] = distance
                else:
                    new_vehicles_and_distances[point["vehicle_ref"]] = distance
                vehicle_data[point["vehicle_ref"]] = (point["line_type"], point["line_name"])

            for veh_id in new_vehicles_and_distances:
                matches_and_distances.append((veh_id, new_vehicles_and_distances[veh_id]))
            collected_matches.append(matches_and_distances)

        # Count the total number of matches
        for matches_and_distances in collected_matches:
            for vehicle in matches_and_distances:
                if vehicle[0] in match_counts:
                    match_counts[vehicle[0]] += 1
                    total_distances[vehicle[0]] += vehicle[1]
                else:
                    match_counts[vehicle[0]] = 1
                    total_distances[vehicle[0]] = vehicle[1]

        # Find the vehicle ids with most matches
        for i in range(MAXIMUM_MASS_TRANSIT_MISSES + 1):
            for match in match_counts:
                if match_counts[match] == NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES - i:
                    final_matches.append(match)
            if len(final_matches) > 0:
                break

        if len(final_matches) == 0:
            return None, None

        #print device_data_queue[0]["time"]
        #for transit in final_matches:
        #    print vehicle_data[transit]

        # Find the vehicle whose total distance from device data samples was the smallest.
        min_distance = -1
        final_vehicle = ""
        for veh_id in final_matches:
            if min_distance < 0:
                min_distance = total_distances[veh_id]
                final_vehicle = veh_id
            else:
                if total_distances[veh_id] < min_distance:
                    min_distance = total_distances[veh_id]
                    final_vehicle = veh_id

        line_data = vehicle_data[final_vehicle]
        #print line_data
        return line_data[0], line_data[1] # line_type, line_name




    def _analyse_row_activities(self, row):
        good_activities = ('IN_VEHICLE', 'ON_BICYCLE', 'RUNNING', 'WALKING', 'ON_FOOT')
        current_activity = "NOT_SET"
        act_1 = row["activity_1"]
        act_2 = row["activity_2"]
        act_3 = row["activity_3"]
        conf_1 = row["activity_1_conf"]
        conf_2 = row["activity_2_conf"]
        conf_3 = row["activity_3_conf"]
        if self._is_duplicate_unsure_row(act_1, act_2, act_3, conf_1, conf_2, conf_3):
            return current_activity
        if conf_3 > 0 and act_3 in good_activities:
            current_activity = act_3
        if conf_2 > 0 and act_2 in good_activities:
            current_activity = act_2
        if conf_1 > 0 and act_1 in good_activities:
            current_activity = act_1
        if current_activity == "ON_FOOT": #The dictionary performs this check in get_best_activity
            current_activity = "WALKING"
        return current_activity

    def _is_duplicate_unsure_row(self, act_1, act_2, act_3, conf_1, conf_2, conf_3):

        if (conf_1 < 100 and
                self.previous_activity_1 == act_1 and
                self.previous_activity_2 == act_2 and
                self.previous_activity_3 == act_3 and
                self.previous_activity_1_conf == conf_1 and
                self.previous_activity_2_conf == conf_2 and
                self.previous_activity_3_conf == conf_3):
            return True

        self.previous_activity_1 = act_1
        self.previous_activity_2 = act_2
        self.previous_activity_3 = act_3
        self.previous_activity_1_conf = conf_1
        self.previous_activity_2_conf = conf_2
        self.previous_activity_3_conf = conf_3

        return False


    def analyse_unfiltered_data(self, device_data_rows, user_id):
        rows = device_data_rows.fetchall()
        if len(rows) == 0:
            return
        device_data_queue = []
        previous_device_id = rows[0]["device_id"]
        previous_time = rows[0]["time"]
        previous_activity = "NOT_SET"
        chosen_activity = "NOT_SET"
        consecutive_differences = 0
        match_counter = 0
        different_activity_counter = 0

        for i in xrange(len(rows)):
            current_row = rows[i]
            if (current_row["time"] - previous_time).total_seconds() > MAX_POINT_TIME_DIFFERENCE:
                if chosen_activity != "NOT_SET": #if false, no good activity was found
                    self._flush_device_data_queue(device_data_queue, chosen_activity, user_id)
                device_data_queue = []
                previous_device_id = current_row["device_id"]
                previous_time = current_row["time"]
                previous_activity = "NOT_SET"
                chosen_activity = "NOT_SET"
                match_counter = 0

            current_activity = self._analyse_row_activities(current_row)

            if previous_device_id != current_row["device_id"] and \
                            (current_row["time"] - previous_time).total_seconds() < MAX_DIFFERENT_DEVICE_TIME_DIFFERENCE:
                if previous_device_id > current_row["device_id"]:
                    device_data_queue.pop()
                else:
                    continue

            device_data_queue.append(current_row)

            if current_activity != "NOT_SET":
                if previous_activity == current_activity:
                    match_counter += 1
                    if match_counter == CONSECUTIVE_DIFFERENCE_LIMIT:
                        chosen_activity = current_activity
                else:
                    match_counter = 0

            #counts the bad activities between two activities. Almost the same as consecutive_differences
            #but counts also bad activities.
            if chosen_activity != "NOT_SET" and current_activity != chosen_activity:
                different_activity_counter += 1
            else:
                different_activity_counter = 0

            #consecutive_differences counts good activities in a row that are different from chosen activity
            if chosen_activity != "NOT_SET" and current_activity != chosen_activity and current_activity != "NOT_SET":
                consecutive_differences += 1
            else:
                consecutive_differences = 0

            if consecutive_differences == CONSECUTIVE_DIFFERENCE_LIMIT:
                #split the transition points, first half is previous activity, latter half is new activity
                splitting_point = CONSECUTIVE_DIFFERENCE_LIMIT + (different_activity_counter - CONSECUTIVE_DIFFERENCE_LIMIT) / 2
                self._flush_device_data_queue(device_data_queue[:-splitting_point], chosen_activity, user_id)
                device_data_queue = device_data_queue[-splitting_point:]
                consecutive_differences = 0
                different_activity_counter = 0
                chosen_activity = current_activity

            previous_device_id = current_row["device_id"]
            previous_time = current_row["time"]
            previous_activity = current_activity

        if chosen_activity != "NOT_SET":
            self._flush_device_data_queue(device_data_queue, chosen_activity, user_id)
