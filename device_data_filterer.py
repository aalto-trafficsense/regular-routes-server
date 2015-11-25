import datetime
from constants import *
import json


class DeviceDataFilterer:

    def __init__(self, db, filtered_device_data_table):
        self.db = db
        self.filtered_device_data_table = filtered_device_data_table
        self.previous_activity_1 = "NOT_SET"
        self.previous_activity_2 = "NOT_SET"
        self.previous_activity_3 = "NOT_SET"
        self.previous_activity_1_conf = 0
        self.previous_activity_2_conf = 0
        self.previous_activity_3_conf = 0

    def flush_device_data_queue(self, device_data_queue, activity, device_id):
        if len(device_data_queue) == 0:
            return
        filtered_device_data = []
        for device_data_row in device_data_queue:
            current_location = json.loads(device_data_row["geojson"])["coordinates"]
            filtered_device_data.append({"activity" : activity,
                                         "device_id" : device_id,
                                         'coordinate': 'POINT(%f %f)' % (float(current_location[0]), float(current_location[1])),
                                         "time" : device_data_row["time"],
                                         "waypoint_id" : device_data_row["waypoint_id"]})

        self.db.engine.execute(self.filtered_device_data_table.insert(filtered_device_data))


    def analyse_row_activities(self, row):
        good_activities = ('IN_VEHICLE', 'ON_BICYCLE', 'RUNNING', 'WALKING', 'ON_FOOT')
        current_activity = "NOT_SET"
        act_1 = row["activity_1"]
        act_2 = row["activity_2"]
        act_3 = row["activity_3"]
        conf_1 = row["activity_1_conf"]
        conf_2 = row["activity_2_conf"]
        conf_3 = row["activity_3_conf"]
        #if self.is_duplicate_unsure_row(act_1, act_2, act_3, conf_1, conf_2, conf_3):
        #    return current_activity
        if conf_3 > 0 and act_3 in good_activities:
            current_activity = act_3
        if conf_2 > 0 and act_2 in good_activities:
            current_activity = act_2
        if conf_1 > 0 and act_1 in good_activities:
            current_activity = act_1
        if current_activity == "ON_FOOT": #The dictionary performs this check in get_best_activity
            current_activity = "WALKING"
        return current_activity

    def is_duplicate_unsure_row(self, act_1, act_2, act_3, conf_1, conf_2, conf_3):

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


    def analyse_unfiltered_data(self, device_data_rows, device_id):
        rows = device_data_rows.fetchall()
        if len(rows) == 0:
            return
        device_data_queue = []
        time_previous = rows[0]["time"]
        chosen_activity = "NOT_SET"
        consecutive_differences = 0
        previous_activity = "NOT_SET"
        match_counter = 0
        different_activity_counter = 0



        for i in xrange(len(rows)):
            current_row = rows[i]
            if (current_row["time"] - time_previous).total_seconds() > MAX_POINT_TIME_DIFFERENCE:
                if chosen_activity != "NOT_SET": #if false, no good activity was found
                    pass
                    self.flush_device_data_queue(device_data_queue, chosen_activity, device_id)
                device_data_queue = []
                chosen_activity = "NOT_SET"
                match_counter = 0


            time_previous = current_row["time"]
            current_activity = self.analyse_row_activities(current_row)
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
                self.flush_device_data_queue(device_data_queue[:-splitting_point], chosen_activity, device_id)
                device_data_queue = device_data_queue[-splitting_point:]
                consecutive_differences = 0
                different_activity_counter = 0
                chosen_activity = current_activity
            previous_activity = current_activity

        if chosen_activity != "NOT_SET":
            self.flush_device_data_queue(device_data_queue, chosen_activity, device_id)
            pass
