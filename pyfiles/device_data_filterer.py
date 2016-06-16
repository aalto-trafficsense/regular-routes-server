import json

from pyfiles.common_helpers import get_distance_between_coordinates
from pyfiles.constants import (
    ACTIVITY_WIN,
    CONSECUTIVE_DIFFERENCE_LIMIT,
    MAXIMUM_MASS_TRANSIT_MISSES,
    MAX_DIFFERENT_DEVICE_TIME_DIFFERENCE,
    MAX_POINT_TIME_DIFFERENCE,
    NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES)
from pyfiles.database_interface import (
    device_data_filtered_table_insert, get_mass_transit_points)
from pyfiles.mass_transit_match_planner import (
    find_same_journey_time_this_week, match_tripleg_with_publictransport, TripMatchedWithPlannerResult, PlannedTrip, minSpeeds)

# if enabled, records matching results of each user in a separate csv file
DUMP_CSV_FILES = False

class DeviceDataFilterer:

    def __init__(self):
        self.previous_activity_1 = "NOT_SET"
        self.previous_activity_2 = "NOT_SET"
        self.previous_activity_3 = "NOT_SET"
        self.previous_activity_1_conf = 0
        self.previous_activity_2_conf = 0
        self.previous_activity_3_conf = 0
        self.user_invehicle_triplegs = 0 # mehrdad: count in_vehicle trip-legs detected for each user
        self.user_invehicle_triplegs_updated = 0
        self.file_triplegs = None
        self.file_finalstats = None

	# mehrdad: note: works on one trip-leg with specific activity of specific userid (trip-legs found by analyze_unfiltered_data())
    def _flush_device_data_queue(self, device_data_queue, activity, user_id):
        if len(device_data_queue) == 0:
            return

        line_type, line_name = self._match_mass_transit_live(
            activity, device_data_queue)

        line_type, line_name = self._match_mass_transit_planner(
            activity, device_data_queue, user_id, line_type, line_name)

        self._write_filtered_data(
            device_data_queue, activity, user_id, line_type, line_name)


    def _match_mass_transit_planner(
            self, activity, device_data_queue, user_id, line_type, line_name):

        HSL_ERROR_CODE_DATE_TOO_FAR = 406
            
        if activity == "IN_VEHICLE":    # TODO : only for IN_VEHICLE activity? ... but maybe a bus or tram ride is sometimnes misdetected as CYCLING or WALK?

            self.user_invehicle_triplegs += 1
            trip_leg_points = len(device_data_queue)                                  
            print ""
            line_name_str = "None"
            if line_name: line_name_str = line_name.encode('utf-8')
            print "----- TRIP-LEG (in_vehicle) #", self.user_invehicle_triplegs ,"(from first filter detection): ", user_id, activity, line_type, line_name_str
            print "trip-leg starts. ends: ", device_data_queue[0]['time'], " --> ", device_data_queue[trip_leg_points-1]['time'] \
                    , "(poitns in this trip leg:", trip_leg_points, ")"
            
            # detect starting point and end point of the trip-leg":
            start_row = device_data_queue[0]
            end_row = device_data_queue[trip_leg_points-1]

            # do the rest ...:
            start_location = json.loads(start_row["geojson"])["coordinates"]
            end_location = json.loads(end_row["geojson"])["coordinates"]
            start_time  = start_row['time']
            end_time  = end_row['time']
            distance = get_distance_between_coordinates(start_location, end_location) # TODO: should get 'distance' value from the calculated more realistic traveled distances
            duration = end_time - start_time
            avgspeed = distance/duration.total_seconds()
            print "duration:", duration, ",   ", "point-to-point straight-line distance:", distance, "(meters)  =>  ", "straigh-line avgspeed:",avgspeed, "(m/s)"
            
            # mehrdad: trip-leg matching logic *:

            tripleg_updated = 0
            start_location_str='{1},{0}'.format(start_location[0],start_location[1])
            end_location_str='{1},{0}'.format(end_location[0],end_location[1])
            
            matchres = TripMatchedWithPlannerResult()
            
            # if we already got the linename and linetype hsl live, that's more accurate --> we skip the following trip-leg-matching code
            # less than 200 meter, not a big impact! --> ignore            
            if (line_type==None or (line_name=='' or line_name==None)) and distance > 200:
                # try to match this trip-leg with a public transport ride (using hsl query)
                # print "we're sending this to match function:", start_location_str, end_location_str, start_time, end_time
                res, matchres = match_tripleg_with_publictransport(start_location_str, end_location_str, start_time, end_time, device_data_queue)
                
                if res == HSL_ERROR_CODE_DATE_TOO_FAR: # second try (adjust the old weekday to current week)
                    print ""
                    print "failed because: HSL_ERROR_CODE_DATE_TOO_FAR !, trying second time with current week..."
                    starttime_thisweek = find_same_journey_time_this_week(start_time)
                    endtime_thisweek = find_same_journey_time_this_week(end_time)
                    res, matchres = match_tripleg_with_publictransport(start_location_str, end_location_str, starttime_thisweek, endtime_thisweek, device_data_queue)
                                                
                # if managed to match the trip-leg with one public transport ride using HSL query                                                
                if res == 1 and matchres.matchcount > 0: 
                    if matchres.trip.linetype=="RAIL": # our database knows "TRAIN" (defined in 
                        matchres.trip.linetype="TRAIN"
                    # now update the previously 'misdetected' trip                        
                    if matchres.trip.deltaStartPassed and matchres.trip.matchedbyroute: # only if all conditions apply #TODO refactor later
                        line_type = matchres.trip.linetype
                        line_name = matchres.trip.linename
                        self.user_invehicle_triplegs_updated += 1
                        tripleg_updated = 1
                        if line_name: line_name_str = line_name.encode('utf-8')                    
                        print "> TRIP-LEG UPDATED (from second filter detection): ", user_id, activity, line_type, line_name_str

            # other filters: 
            
            # if movement very slow ... probably this is not in_vehicle
            # TODO: but why such slow moevment was detected as 'in_vehicle' in the first place?!
            if (line_type==None or (line_name=='' or line_name==None)) and avgspeed < minSpeeds['walk']:
                print "trip's avg. speed:", avgspeed, "m/s", ": too slow! probably NOT IN_VEHICLE!"
        
            # save the trip-leg records in file *:
            # file columns: 
            #   userid, triplegno, startcoo, endcoo, starttime, endtime, mode, linetype, linename, distance, duration, avgspeed, updated        
            row_basics_str = "{0};{1};{2};{3};{4};{5};{6};{7};{8};{9};{10};{11};{12}".format(\
                                user_id, self.user_invehicle_triplegs, start_location_str, end_location_str, start_time, end_time,\
                                activity, line_type, line_name_str, round(distance), duration, round(avgspeed,1), tripleg_updated)                                
            row_plandetails_str = ""
            if matchres.matchcount > 0:
                row_plandetails_str = "{0};{1};{2};{3};{4};{5};{6};{7};;{8};{9};{10};{11};{12}".format(\
                                            matchres.trip.start, matchres.trip.end, 
                                            matchres.trip.legstart, matchres.trip.legend, 
                                            matchres.trip.deltaT, matchres.trip.deltaTsign, 
                                            matchres.trip.deltaStarttimeStr, matchres.trip.deltaStartPassed,
                                            matchres.trip.linetype, matchres.trip.linename, 
                                            matchres.trip.matchedbyroute, matchres.trip.matched_fraction, matchres.trip.longest_serialunmatch)
            triplegfileline = row_basics_str + ";;" + row_plandetails_str
                                                        
            if DUMP_CSV_FILES:
                self.file_triplegs.write(triplegfileline+"\n")

        return line_type, line_name


    def _write_filtered_data(
            self, device_data_queue, activity, user_id, line_type, line_name):

        filtered_device_data = []

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


    def _match_mass_transit_live(self, activity, device_data_queue):
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


    def _analyse_activities(self, points):

        def activities(point):
            return {
                point["activity_1"]: point["activity_1_conf"] or 0,
                point["activity_2"]: point["activity_2_conf"] or 0,
                point["activity_3"]: point["activity_3_conf"] or 0}

        def dseconds(p0, p1):
            return (p1["time"] - p0["time"]).total_seconds()

        good_activities = ('IN_VEHICLE', 'ON_BICYCLE', 'RUNNING', 'WALKING')
        on_foot_activities = ('RUNNING', 'WALKING')

        def best_activity(activities):
            on_foot = False
            for activity, cconf in activities.most_common():
                if activity == "ON_FOOT":
                    on_foot = True
                elif on_foot and activity not in on_foot_activities:
                    pass
                elif activity in good_activities:
                    return activity
            if on_foot:
                return "WALKING"
            return "NOT_SET"

        from collections import Counter
        probs = Counter()
        head = tail = 0
        halfwin = ACTIVITY_WIN / 2
        np = len(points)
        for i in range(np):
            while head < np and dseconds(points[i], points[head]) < halfwin:
                probs.update(activities(points[head]))
                head += 1
            while tail < np and dseconds(points[tail], points[i]) > halfwin:
                probs.subtract(activities(points[tail]))
                tail += 1
            yield points[i], best_activity(probs)


    def _dump_csv_file_open(self, user_id):
        self.user_invehicle_triplegs = 0
        self.user_invehicle_triplegs_updated = 0
        filename = "user_{0}_triplegs_invehicle.csv".format(user_id)
        self.file_triplegs = open(filename, 'a')
        maincols = "userid;triplegno;startCoo;endCoo;starttime;endtime;mode;linetype;linename;distance;duration;avgspeed;updated"        
        plandetails = "planned_start;planned_end;transit_start;transit_end;delta_trip_duration;planned_trip_is_shorter;delta_transit_starttime;delta_passed?\
                        ;;;;route_matched?;point_match_fraction;longest_serialunmatch"        
        self.file_triplegs.write(maincols + ";<<<>>>;" + plandetails+"\n")



    def analyse_unfiltered_data(self, device_data_rows, user_id):
        if DUMP_CSV_FILES:
            self._dump_csv_file_open(user_id)

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

        for current_row, current_activity in self._analyse_activities(rows):
            if (current_row["time"] - previous_time).total_seconds() > MAX_POINT_TIME_DIFFERENCE:
                if chosen_activity != "NOT_SET": #if false, no good activity was found
                    self._flush_device_data_queue(device_data_queue, chosen_activity, user_id)
                device_data_queue = []
                previous_device_id = current_row["device_id"]
                previous_time = current_row["time"]
                previous_activity = "NOT_SET"
                chosen_activity = "NOT_SET"
                match_counter = 0

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

        if DUMP_CSV_FILES:
            self._dump_csv_file_close(user_id)


    def _dump_csv_file_close(self, user_id):    
        self.file_triplegs.close()    
        
        # create the stats file for each userid:
        updated_fraction = self.user_invehicle_triplegs and int(round( (float(self.user_invehicle_triplegs_updated)/self.user_invehicle_triplegs) * 100))
        
        filename = "user_{0}_stats_invehicle.csv".format(user_id)        
        self.file_finalstats = open(filename, 'a')
        self.file_finalstats.write("user_id;triplegs;triplegs_updated;updated_fraction" + "\n")
        fileline = "{0};{1};{2};{3}".format(user_id, self.user_invehicle_triplegs, self.user_invehicle_triplegs_updated, updated_fraction)
        self.file_finalstats.write(fileline + "\n")
        self.file_finalstats.close()
        print ""
        print "--------- STATS FOR user", user_id, "---------"         
        print "total trip-legs (in_vehicle) for this user:", self.user_invehicle_triplegs
        print "updated trip-legs (in_vehicle) to public transport:", self.user_invehicle_triplegs_updated
        print "ratio:", updated_fraction, "%"
        print ""
        
