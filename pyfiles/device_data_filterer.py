import json
import datetime
from sqlalchemy.sql import text
from copy import deepcopy

from pyfiles.common_helpers import (get_distance_between_coordinates, geoJSON_to_pointRow, pointRow_to_geoText, geoJSON_to_geoText, DateTime_to_Text, DateTimeDelta_to_Text, DateTime_to_SqlText)
from pyfiles.common_helpers import (pointRow_to_geoText, pointRow_to_postgisPoint, round_dict_values, DateTime_to_Text, DateTimeDelta_to_Text, dict_to_sqlstr, dict_timedelta_to_text)
from pyfiles.logger import(log, logi, loge)

from pyfiles.constants import (
    ACTIVITY_WIN,
    CONSECUTIVE_DIFFERENCE_LIMIT,
    MAXIMUM_MASS_TRANSIT_MISSES,
    MAX_DIFFERENT_DEVICE_TIME_DIFFERENCE,
    MAX_MASS_TRANSIT_DISTANCE_DIFFERENCE,
    MAX_MASS_TRANSIT_TIME_DIFFERENCE,
    MAX_POINT_TIME_DIFFERENCE,
    NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES)

from pyfiles.database_interface import (
    device_data_filtered_table_insert, match_mass_transit_live)
from pyfiles.mass_transit_match_planner import (
    find_same_journey_time_this_week, MassTransitMatchPlanner, TripMatchedWithPlannerResult, PlannedTripLeg, minSpeeds)
from pyfiles.trip_economics import (TripEconomics)
from trip_planner import(Trip)


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

    # trip extraction ----------------------------------------------------------------
    # works based on a state-diagram approach:
    # 'actvity location' is a state and 'leg' is a transition from one state to another
    # note: refer to paper notes
    def get_trips_from_legs(self, db, ids_to_process, date_range_start, date_range_end):        
        qstr = "SELECT id, user_id, device_id, time_start, time_end, ST_AsGeoJSON(coordinate_start) as origin, ST_AsGeoJSON(coordinate_end) destination, " \
                "activity, line_type, line_name, line_source, time_end - time_start as duration " \
                "FROM legs WHERE user_id IN({0}) AND time_start >= '{1}' AND time_end <= '{2}' "\
                "ORDER BY user_id, time_start ;".format(ids_to_process, str(date_range_start), str(date_range_end))
                # TODO! IMPORTANT ... REMOVE : Order by device_id
        logi(["get_trips_from_legs:: qstr:", qstr])
        logi("")
        legs =  db.engine.execute(text(qstr))                                    

        trips = []
        trip = None        
        # leg_index = 0 #TODO: not needed anymore?
        lastleg = None
        time_distance_threshold = datetime.timedelta(minutes = 15)
        at_activity_location = True # first trip (of each user*) starts with first leg record (one reason is to avoid 'shifted trip extraction')
        at_new_user = False
        lastuserid = None

        # assumptions, constants, adjusting parameters, related cals, some kinematics, etc. -----------------:
        # TODO!!! MOVE these to constants.py **
        # TODO, NOTE!: are there more problems in detecting trip_started_here (because of mode detection delay) ??? 
        MAX_MODE_DETECTION_DELAY = 500 # (meters) we have latency in making sure of the mode change
        MAX_GPS_ERROR = 50 # (in meters) if error marger than this, we've discarded that point TODO ???
        MAX_VEHICLE_LENGTH = 50 # TODO
        MAX_DISTANCE_FOR_POINT_MATCH = MAX_GPS_ERROR + MAX_VEHICLE_LENGTH
        
        for leg in legs:                
            #print leg['time_start'], leg['time_end'], leg['activity'], leg['line_type']
                        
            if leg['activity'] != 'STILL': # skip STILL legs
                # detect arriving at a new user's leg record *:
                if leg['user_id'] != lastuserid:
                     # reset trip_id and other params for the new user:
                    trip_id_index = self.get_next_user_trip_id(db, leg['user_id'], date_range_start, date_range_end)
                    lastuserid = leg['user_id']
                    at_new_user = True
            
                # detect 'activity locations' *: 
                if not at_new_user and lastleg is not None: 
                    legs_time_distance = leg['time_start'] - lastleg['time_end']
                    if legs_time_distance > time_distance_threshold: #TODO and other conditions ...                        
                        at_activity_location = True # last trip ends here & next trip starts here
                    else:
                        at_activity_location = False
                
                # act accordingly at 'activity location' or when a new user
                if at_new_user or at_activity_location:
                    if trip is not None: # if we had started a trip and it's still open-ended
                        # end current trip here: 
                        trip.destination = geoJSON_to_pointRow(lastleg['destination'])
                        trip.endtime = lastleg['time_end'] 
                        trips.append(trip) # add it to the collection of extracted trips *
                        trip = None
                    # start a new trip here:
                    trip_id_index += 1
                    trip = Trip()
                    trip.user_id = leg['user_id']
                    trip.device_id = leg['device_id']                    
                    trip.id = trip_id_index
                    trip.plan_id = 0 # this is the actual trip (we're processing only the actual recorded trips in this function)
                    trip.origin = geoJSON_to_pointRow(leg['origin'])
                    trip.starttime = leg['time_start']

                if trip is not None: #save non-STILL legs to the trip
                    trip.append_trafficsense_leg(leg)

                #leg_index += 1 # TODO: not needed anymore?
                lastleg = leg      
                if at_new_user:                       
                    at_new_user = False
        #loop ends------------------------
        # deal with the last remaining leg:             
        if trip is not None: # if we've started a trip and it's still open-ended
            # end current trip here
            trip.destination = geoJSON_to_pointRow(lastleg['destination'])
            trip.endtime = lastleg['time_end']          
            trips.append(trip)
                    
        print "Number of extracted trips:", len(trips)
        print "Showing the extracted trips:"
        for trip in trips:
            self.display_trip_and_legs(trip)

        return 1, trips

    def get_next_user_trip_id(self, db, user_id, date_range_start, date_range_end):
        qstr = """SELECT max(id) as max_id
                    FROM trips  
                    WHERE user_id = {0} AND (user_id, id, plan_id) NOT IN
                    (SELECT user_id, id, plan_id FROM trips
                     WHERE user_id = {0}
                     AND start_time >= '{1}' AND end_time <= '{2}');
                """.format(user_id, DateTime_to_Text(date_range_start), DateTime_to_Text(date_range_end))                                            
        log(["load_trips():: qstr:", qstr])
        log("")
        maxid_rows =  db.engine.execute(text(qstr))   
        
        if maxid_rows.rowcount > 0:
            for maxid_row in maxid_rows: 
                if maxid_row['max_id'] == None:
                    return 0
                else:
                    return maxid_row['max_id']
        
        return 0

    # find an origin/destination (od) previosuly detected and stored. match in certain proximity
    def find_trip_od_for_recorded_location(self, recorded_geoloc, distance_inaccuracy_threshold):        
        qstr = """SELECT geoloc 
                    FROM trip_origin_destinations  
                    WHERE ST_Distance(geoloc, ST_GeomFromText('{0}')) <= {1}
                    """.format(pointRow_to_postgisPoint(recorded_geoloc), distance_inaccuracy_threshold)  
        log(["load_trips():: qstr:", qstr])
        log("")
        trip_od_rows =  db.engine.execute(text(qstr))   
        
        if trip_od_rows.rowcount > 0:
            for trip_od_row in trip_od_rows: 
                return trip_od_row['geoloc']
        
        return None

    def store_trip_od_location(self, recorded_geoloc, distance_inaccuracy_threshold): 
        qstr = """INSERT INTO trip_origin_destinations (geoloc, distance_inaccuracy_threshold)
                        VALUES (ST_GeomFromText('{0}'), {1})
                        """.format(pointRow_to_postgisPoint(recorded_geoloc), distance_inaccuracy_threshold) 
        log(["store_trip:: qstr:", qstr])
        log("")
        
        res = db.engine.execute(text(qstr))
        log(["result:", str(res)]) 

    def display_trip_and_legs(self, trip):
        # TODO temporary: disabled
        return 1
        print trip.user_id,"|",trip.id, ",", trip.plan_id, "|", DateTime_to_Text(trip.starttime), "to", DateTime_to_Text(trip.endtime), \
              "| from", pointRow_to_geoText(trip.origin), "--> to", pointRow_to_geoText(trip.destination)
        for leg in trip.legs:
            print "\t", DateTime_to_Text(leg['time_start']), "to", DateTime_to_Text(leg['time_end']), "|", leg['activity'], leg['line_type'], \
                  "| from", geoJSON_to_geoText(leg['origin']), "--> to", geoJSON_to_geoText(leg['destination']), "| duration:", DateTimeDelta_to_Text(leg['duration'])

    def delete_trips(self, db, ids_to_process, date_range_start, date_range_end):
        qstr = """DELETE FROM trips 
                    WHERE user_id IN ({0})
                    AND start_time >= '{1}' AND end_time <= '{2}';
                """.format(ids_to_process, DateTime_to_Text(date_range_start), DateTime_to_Text(date_range_end))
        logi(["delete_trips():: qstr for removing old rows before storing new ones:", qstr])
        res = db.engine.execute(text(qstr))
        logi(["result:", res])
        logi("")
        
    def load_trips(self, db, ids_to_process, date_range_start, date_range_end,  timeofday_start,  timeofday_end):        
        qstr = """SELECT user_id, device_id, id, plan_id, start_time, end_time, ST_AsGeoJSON(origin) as origin, ST_AsGeoJSON(destination) as destination, 
                        multimodal_summary, duration, cost, calories, emission, comfort, distance, 
                        time_by_mode, cost_by_mode, calories_by_mode, emission_by_mode, distance_by_mode,
                        mainmode, 
                        start_time_for_plan, notes
                    FROM trips  
                    WHERE (user_id, id) IN
                    ( SELECT user_id, id FROM trips 
                      WHERE user_id IN({0}) AND plan_id = 0 
                      AND start_time >= '{1}' AND end_time <= '{2}' 
                      AND start_time::time >= time {3} AND start_time::time <= time {4}
                      AND end_time::time >= time {3} AND end_time::time <= time {4} )
                    ORDER BY user_id, id, plan_id;
                    """.format(ids_to_process, DateTime_to_Text(date_range_start), DateTime_to_Text(date_range_end), 
                               DateTime_to_SqlText(timeofday_start), DateTime_to_SqlText(timeofday_end))                                             
        log(["load_trips():: qstr:", qstr])
        log("")        
        trip_rows =  db.engine.execute(text(qstr))   
        
        trips = []
        if trip_rows.rowcount > 0:
            actualtrip = None
            for trip_row in trip_rows: 
                trip = Trip(trip_row)
                # OLD Code : Parse JSON into an object with attributes corresponding to dict keys
                #   TODO! WARNING: (?) only works well if names of DB table columns are same as names of our Python Class attributes
                #   trip.__dict__= json.loads(trip_row['trip_as_json'])
                
#                trip.user_id = trip_row['user_id']
#                trip.device_id = trip_row['device_id']                
#                trip.id = trip_row['id']
#                trip.plan_id = trip_row['plan_id']
#                
#                trip.origin = geoJSON_to_pointRow(trip_row['origin'])
#                trip.destination = geoJSON_to_pointRow(trip_row['destination'])
#                trip.starttime = trip_row['start_time']
#                trip.endtime = trip_row['end_time']
#
#                trip.legs = [] # collection of type 'legs' table row
#
#                # trip economics values:
#                trip.distance = trip_row['distance']
#                trip.duration = trip_row['duration']
#                trip.cost = trip_row['cost']
#                trip.calories = trip_row['calories']
#                trip.emission = trip_row['emission']
#                trip.comfort = trip_row['comfort']
#                # TODO following !!!
#                trip.distance_by_mode = json.loads(trip_row['distance_by_mode'])
#                trip.duration_by_mode = json.loads(trip_row['time_by_mode'].replace("'", '"'))
#                trip.cost_by_mode = json.loads(trip_row['cost_by_mode'].replace("'", '"'))
#                trip.calories_by_mode =json.loads(trip_row['calories_by_mode'].replace("'", '"'))
#                trip.emission_by_mode =json.loads(trip_row['emission_by_mode'].replace("'", '"'))
#                
#                trip.alternative_trips = [] # a collection of type:class Trip
#                trip.user = None # TODO is this used now?!
#                trip.has_transitLeg = False # TODO, what to set?!!
#                
#                trip.multimodal_summary = trip_row['multimodal_summary']
#                trip.mainmode = trip_row['mainmode']        
#                
#                trip.shifted_starttime_for_publictransport_tripplan = trip_row['start_time_for_plan']
                                
                # TODO: load trip legs *
                # ...
                
                # build the nested structure of 'trips' list in our program:
                if trip.plan_id == 0:
                    actualtrip = Trip()            
                    actualtrip = deepcopy(trip)
                    trips.append(actualtrip)
                elif trip.plan_id > 0:
                    if actualtrip is not None:
                        actualtrip.alternative_trips.append(trip)
                    
                # print trip.user_id,  trip.id,  trip.plan_id,  trip.origin                                
        
        return trips

    def store_trips(self, db, ids_to_process, trips):        
        for trip in trips:
            self.store_trip(db, trip)
            for tripalt in trip.alternative_trips:
                self.store_trip(db, tripalt)        
                
    def store_trip(self, db, trip):
        # NOTE: example INSERT: insert into mytest (name) VALUES ('{''car'', ''walk''}'); 
        
        qstr = """INSERT INTO trips (user_id, device_id, id, plan_id, start_time, end_time, 
               origin, destination, 
               multimodal_summary, 
               duration, cost, calories, emission, comfort, distance, time_by_mode, cost_by_mode, calories_by_mode, emission_by_mode, distance_by_mode, mainmode,start_time_for_plan) 
               VALUES ({0},{1},{2},{3},'{4}','{5}',
               ST_GeomFromText('{6}'), ST_GeomFromText('{7}'),
               '{8}',
               '{9}',{10},{11},{12},{13},{14},
               '{15}','{16}','{17}','{18}','{19}',
               '{20}',
               {21})""".format(
               trip.user_id, trip.device_id, trip.id, trip.plan_id, DateTime_to_Text(trip.starttime), DateTime_to_Text(trip.endtime), 
               pointRow_to_postgisPoint(trip.origin), pointRow_to_postgisPoint(trip.destination),
               trip.multimodal_summary, 
               DateTimeDelta_to_Text(trip.duration), 
               round(trip.cost, 2), 
               int(round(trip.calories)), 
               round(trip.emission, 1),  # TODO, old: round(trip.emission/1000.0, 1), 
               trip.comfort, 
               round(trip.distance, 1),  # TODO
               json.dumps(dict_timedelta_to_text(trip.duration_by_mode)),   #TODO, test and verify all following
               json.dumps(round_dict_values(trip.cost_by_mode, 2)), 
               json.dumps(round_dict_values(trip.calories_by_mode, 2)), 
               json.dumps(round_dict_values(trip.emission_by_mode, 2)), 
               json.dumps(round_dict_values(trip.distance_by_mode, 2)), 
               trip.mainmode, 
               DateTime_to_SqlText(trip.shifted_starttime_for_publictransport_tripplan)
               )
               #round_dict_values(trip.duration_by_mode, 2), \
               #round_dict_values(trip.cost_by_mode, 2),  round_dict_values(trip.calories_by_mode, 0), \
               #round_dict_values(trip.emission_by_mode, 0), round_dict_values(trip.distance_by_mode, 0),\
               #pointRow_to_geoText(trip.origin), pointRow_to_geoText(trip.destination)               
        log(["store_trip:: qstr:", qstr])
        log("")
        
        res = ""
        try:
            res = db.engine.execute(text(qstr))
        except Exception as e:
            print ""
            print ">> store_trip():: FAILED ------------------------------"
            print ">> trip to store:", trip
            print ".............................................."
            print ">> qstr:", qstr
            print ".............................................."            
            print ">> (!) EXCEPTION catched: ", e
            print ""
            print ""
        log(["result:", str(res)])
        

    def delete_trips_to_legs(self, db, ids_to_process, date_range_start, date_range_end):
        qstr = """DELETE FROM trips_to_legs 
                    WHERE (user_id, trip_id, plan_id) IN
                    (SELECT user_id, id, plan_id FROM trips
                     WHERE user_id IN ({0})
                     AND start_time >= '{1}' AND end_time <= '{2}'
                    );
                """.format(ids_to_process, DateTime_to_Text(date_range_start), DateTime_to_Text(date_range_end))
        logi(["delete_trips_to_legs():: qstr for removing old rows before storing new ones:", qstr])
        res = db.engine.execute(text(qstr))
        logi(["result:", str(res)])
        logi("")

    def store_trips_to_legs(self, db, ids_to_process, trips):        
        for trip in trips:
            for leg in trip.legs:
                self.store_trip_to_leg(db, trip,  leg)
                
            # TOOD: we don't save legs of alternative trips for now (keep it simple) ... should do later?
            #for tripalt in trip.alternative_trips:
            #    self.store_trip_to_leg(db, tripalt)        
    
    def store_trip_to_leg(self, db, trip,  leg):        
        qstr = """INSERT INTO trips_to_legs (user_id, trip_id, plan_id, leg_id) 
               VALUES ({0},{1},{2},{3})""".format(
               trip.user_id, trip.id, trip.plan_id,  leg['id'])

        log(["store_trip_to_leg():: qstr:", qstr])
        log("")
        
        res = db.engine.execute(text(qstr))
        log(["result:", str(res)])
        
    
    # -------------------------------------------------------------------------------
        
	# note: this function works on one trip-leg with specific activity of specific userid (trip-legs found by analyze_unfiltered_data())
    def _flush_device_data_queue(self, device_data_queue, activity, user_id):
        if len(device_data_queue) == 0:
            return
        
        line_type, line_name = self._match_mass_transit_live(
            activity, device_data_queue)

        line_type, line_name = self._match_mass_transit_planner(
            activity, device_data_queue, user_id, line_type, line_name)

        self._write_filtered_data(
            device_data_queue, activity, user_id, line_type, line_name)
            
    def _match_mass_transit_planner_2(
            self, activity, device_data_queue, user_id, line_type, line_name):

        OTP_ERROR_CODE_DATE_TOO_FAR = 406
        
        # TODO : implement matching for "BIKE" legs too (in case BIKE and IN_VEHICLE are confused by the app): 
            #   ...
            #   check avg speed, etc. ... guess whether this could be public transport, etc.
            #   ...

        # TODO : only for IN_VEHICLE activity? ... but maybe a bus or tram ride is sometimnes misdetected as CYCLING or WALK?            
        if activity == "IN_VEHICLE":    
            self.user_invehicle_triplegs += 1
            trip_leg_points = len(device_data_queue)                                  
            print ""
            line_name_str = "None"
            if line_name: 
                line_name_str = line_name.encode('utf-8')
            print "----- TRIP-LEG (in_vehicle) #", self.user_invehicle_triplegs ,"(from first filter detection): ", user_id, activity, line_type, line_name_str
            print "trip-leg starts. ends: ", device_data_queue[0]['time'], " --> ", device_data_queue[trip_leg_points-1]['time'] \
                    , "(poitns in this trip leg:", trip_leg_points, ")"
            
            # detect starting point and end point of the trip-leg:
            start_row = device_data_queue[0]
            end_row = device_data_queue[trip_leg_points-1]

            # do the rest ...:
            start_location = json.loads(start_row["geojson"])["coordinates"]
            end_location = json.loads(end_row["geojson"])["coordinates"]
            start_time  = start_row['time']
            end_time  = end_row['time']
            distance = get_distance_between_coordinates(start_location, end_location) # TODO: should get 'distance' value from the more realistic calculated traveled distances (however, traveled distances 'distance' value seems to be also point-to-point for now)
            duration = end_time - start_time
            avgspeed = distance/duration.total_seconds()
            print "duration:", duration, ",   ", "point-to-point straight-line distance:", distance, "(meters)  =>  ", "straigh-line avgspeed:",avgspeed, "(m/s)"
            
            # mehrdad: trip-leg matching logic * -----------------------------------------:
            tripleg_updated = 0
            start_location_str='{1},{0}'.format(start_location[0],start_location[1])
            end_location_str='{1},{0}'.format(end_location[0],end_location[1])
            
            matchres = TripMatchedWithPlannerResult()
            plannermatch = MassTransitMatchPlanner()
            
            # if we already got the linename and linetype hsl live, that's more accurate --> we skip the following trip-leg-matching code
            # less than 200 meter, not a big impact! --> ignore            
            if (line_type==None or (line_name=='' or line_name==None)) and distance > 200:
                # try to match this trip-leg with a public transport ride (using hsl query)
                # print "we're sending this to match function:", start_location_str, end_location_str, start_time, end_time
                res, matchres = plannermatch.match_tripleg_with_publictransport(start_location_str, end_location_str, start_time, end_time, device_data_queue)
                
                if res == 0 and matchres.error_code == OTP_ERROR_CODE_DATE_TOO_FAR: # second try (adjust the old weekday to current week)
                    print ""
                    print "FAILED because: OTP_ERROR_CODE_DATE_TOO_FAR !, trying second time with current week..."
                    starttime_thisweek = find_same_journey_time_this_week(start_time)
                    endtime_thisweek = find_same_journey_time_this_week(end_time)
                    res, matchres = plannermatch.match_tripleg_with_publictransport(start_location_str, end_location_str, starttime_thisweek, endtime_thisweek, device_data_queue)

                if res == 1 and matchres.matchcount == 0:
                    print ""
                    print "> NO matching transit detected. (matchcount:",matchres.matchcount,")"
                # if managed to match the trip-leg with one public transport ride using HSL query                                                
                elif res == 1 and matchres.matchcount > 0: 
                    if matchres.trip.linetype=="RAIL": # our database knows "TRAIN" (defined in 
                        matchres.trip.linetype="TRAIN"
                    # now update the previously 'misdetected' trip                        
                    if matchres.trip.deltaStartPassed and matchres.trip.matchedbyroute: # only if all conditions apply #TODO refactor later
                        line_type = matchres.trip.linetype
                        line_name = matchres.trip.linename
                        self.user_invehicle_triplegs_updated += 1
                        tripleg_updated = 1
                        if line_name: 
                            line_name_str = line_name.encode('utf-8')                    
                        print "> TRIP-LEG UPDATED (from second filter detection): ", user_id, activity, line_type, line_name_str
                    else:
                        print ""
                        print "> NO matching transit detected. (matchcount:",matchres.matchcount,", deltaStartPassed:",matchres.trip.deltaStartPassed,", matchedbyroute",matchres.trip.matchedbyroute,")"
                else:
                    print ""
                    print "> FAILED because: error_code:", matchres.error_code, "error_text1:",matchres.error_msg, "error_text2:",matchres.error_message

            if tripleg_updated == 0:
                print ""
                print "> Nothing updated"
                               

            # other filters: 
            
            # if movement very slow ... probably this is not in_vehicle
            # TODO: but why such slow moevment was detected as 'in_vehicle' in the first place?!
            if (line_type==None or (line_name=='' or line_name==None)) and avgspeed < minSpeeds['walk']:
                print "trip's avg. speed:", avgspeed, "m/s", ": too slow! probably NOT IN_VEHICLE!"
        
            # save the trip-leg records in file -------------------------------------:
            # file columns: 
            #   userid, triplegno, startcoo, endcoo, starttime, endtime, mode, linetype, linename, distance, duration, avgspeed, updated        
            if DUMP_CSV_FILES:
                row_basics_str = "{0};{1};{2};{3};{4};{5};{6};{7};{8};{9};{10};{11};{12}".format(\
                                    user_id, self.user_invehicle_triplegs, start_location_str, end_location_str, start_time, end_time,\
                                    activity, line_type, line_name_str, round(distance), duration, round(avgspeed,1), tripleg_updated)                                
                row_plandetails_str = ""
                if matchres.matchcount > 0:                
                    # TODO fix this better!
                    line_name = matchres.trip.linename
                    if line_name: line_name_str = line_name.encode('utf-8')                    
                
                    row_plandetails_str = "{0};{1};{2};{3};{4};{5};{6};{7};;{8};{9};{10};{11};{12}".format(\
                                                matchres.trip.start, matchres.trip.end, 
                                                matchres.trip.legstart, matchres.trip.legend, 
                                                matchres.trip.deltaT, matchres.trip.deltaTsign, 
                                                matchres.trip.deltaStarttimeStr, matchres.trip.deltaStartPassed,
                                                matchres.trip.linetype, line_name_str, 
                                                matchres.trip.matchedbyroute, matchres.trip.matched_fraction, matchres.trip.longest_serialunmatch)
                triplegfileline = row_basics_str + ";;" + row_plandetails_str
                                                        
                self.file_triplegs.write(triplegfileline+"\n")

        return line_type, line_name

    def _match_mass_transit_planner(
            self, activity, device_data_queue, user_id, line_type, line_name):

        OTP_ERROR_CODE_DATE_TOO_FAR = 406
        
        # TODO : implement matching for "BIKE" legs too (in case BIKE and IN_VEHICLE are confused by the app): 
            #   ...
            #   check avg speed, etc. ... guess whether this could be public transport, etc.
            #   ...

        # TODO : only for IN_VEHICLE activity? ... but maybe a bus or tram ride is sometimnes misdetected as CYCLING or WALK?            
        if activity == "IN_VEHICLE":    
            self.user_invehicle_triplegs += 1
            trip_leg_points = len(device_data_queue)                                  
            print ""
            line_name_str = "None"
            if line_name: 
                line_name_str = line_name.encode('utf-8')
            print "----- TRIP-LEG (in_vehicle) #", self.user_invehicle_triplegs ,"(from first filter detection): ", user_id, activity, line_type, line_name_str
            print "trip-leg starts. ends: ", device_data_queue[0]['time'], " --> ", device_data_queue[trip_leg_points-1]['time'] \
                    , "(poitns in this trip leg:", trip_leg_points, ")"
            
            # detect starting point and end point of the trip-leg:
            start_row = device_data_queue[0]
            end_row = device_data_queue[trip_leg_points-1]

            # do the rest ...:
            start_location = json.loads(start_row["geojson"])["coordinates"]
            end_location = json.loads(end_row["geojson"])["coordinates"]
            start_time  = start_row['time']
            end_time  = end_row['time']
            distance = get_distance_between_coordinates(start_location, end_location) # TODO: should get 'distance' value from the more realistic calculated traveled distances (however, traveled distances 'distance' value seems to be also point-to-point for now)
            duration = end_time - start_time
            avgspeed = distance/duration.total_seconds()
            print "duration:", duration, ",   ", "point-to-point straight-line distance:", distance, "(meters)  =>  ", "straigh-line avgspeed:",avgspeed, "(m/s)"
            
            # mehrdad: trip-leg matching logic * -----------------------------------------:
            tripleg_updated = 0
            start_location_str='{1},{0}'.format(start_location[0],start_location[1])
            end_location_str='{1},{0}'.format(end_location[0],end_location[1])
            
            matchres = TripMatchedWithPlannerResult()
            plannermatch = MassTransitMatchPlanner()
            
            # if we already got the linename and linetype hsl live, that's more accurate --> we skip the following trip-leg-matching code
            # less than 200 meter, not a big impact! --> ignore            
            if (line_type==None or (line_name=='' or line_name==None)) and distance > 200:
                # try to match this trip-leg with a public transport ride (using hsl query)
                # print "we're sending this to match function:", start_location_str, end_location_str, start_time, end_time
                res, matchres = plannermatch.match_tripleg_with_publictransport(start_location_str, end_location_str, start_time, end_time, device_data_queue)
                
                if res == 0 and matchres.error_code == OTP_ERROR_CODE_DATE_TOO_FAR: # second try (adjust the old weekday to current week)
                    print ""
                    print "FAILED because: OTP_ERROR_CODE_DATE_TOO_FAR !, trying second time with current week..."
                    starttime_thisweek = find_same_journey_time_this_week(start_time)
                    endtime_thisweek = find_same_journey_time_this_week(end_time)
                    res, matchres = plannermatch.match_tripleg_with_publictransport(start_location_str, end_location_str, starttime_thisweek, endtime_thisweek, device_data_queue)

                if res == 1 and matchres.matchcount == 0:
                    print ""
                    print "> NO matching transit detected. (matchcount:",matchres.matchcount,")"
                # if managed to match the trip-leg with one public transport ride using HSL query                                                
                elif res == 1 and matchres.matchcount > 0: 
                    if matchres.trip.linetype=="RAIL": # our database knows "TRAIN" (defined in 
                        matchres.trip.linetype="TRAIN"
                    # now update the previously 'misdetected' trip                        
                    if matchres.trip.deltaStartPassed and matchres.trip.matchedbyroute: # only if all conditions apply #TODO refactor later
                        line_type = matchres.trip.linetype
                        line_name = matchres.trip.linename
                        self.user_invehicle_triplegs_updated += 1
                        tripleg_updated = 1
                        if line_name: 
                            line_name_str = line_name.encode('utf-8')                    
                        print "> TRIP-LEG UPDATED (from second filter detection): ", user_id, activity, line_type, line_name_str
                    else:
                        print ""
                        print "> NO matching transit detected. (matchcount:",matchres.matchcount,", deltaStartPassed:",matchres.trip.deltaStartPassed,", matchedbyroute",matchres.trip.matchedbyroute,")"
                else:
                    print ""
                    print "> FAILED because: error_code:", matchres.error_code, "error_text1:",matchres.error_msg, "error_text2:",matchres.error_message

            if tripleg_updated == 0:
                print ""
                print "> Nothing updated"
                               

            # other filters: 
            
            # if movement very slow ... probably this is not in_vehicle
            # TODO: but why such slow moevment was detected as 'in_vehicle' in the first place?!
            if (line_type==None or (line_name=='' or line_name==None)) and avgspeed < minSpeeds['walk']:
                print "trip's avg. speed:", avgspeed, "m/s", ": too slow! probably NOT IN_VEHICLE!"
        
            # save the trip-leg records in file -------------------------------------:
            # file columns: 
            #   userid, triplegno, startcoo, endcoo, starttime, endtime, mode, linetype, linename, distance, duration, avgspeed, updated        
            if DUMP_CSV_FILES:
                row_basics_str = "{0};{1};{2};{3};{4};{5};{6};{7};{8};{9};{10};{11};{12}".format(\
                                    user_id, self.user_invehicle_triplegs, start_location_str, end_location_str, start_time, end_time,\
                                    activity, line_type, line_name_str, round(distance), duration, round(avgspeed,1), tripleg_updated)                                
                row_plandetails_str = ""
                if matchres.matchcount > 0:                
                    # TODO fix this better!
                    line_name = matchres.trip.linename
                    if line_name: line_name_str = line_name.encode('utf-8')                    
                
                    row_plandetails_str = "{0};{1};{2};{3};{4};{5};{6};{7};;{8};{9};{10};{11};{12}".format(\
                                                matchres.trip.start, matchres.trip.end, 
                                                matchres.trip.legstart, matchres.trip.legend, 
                                                matchres.trip.deltaT, matchres.trip.deltaTsign, 
                                                matchres.trip.deltaStarttimeStr, matchres.trip.deltaStartPassed,
                                                matchres.trip.linetype, line_name_str, 
                                                matchres.trip.matchedbyroute, matchres.trip.matched_fraction, matchres.trip.longest_serialunmatch)
                triplegfileline = row_basics_str + ";;" + row_plandetails_str
                                                        
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

        device = device_data_queue[0]["device_id"]
        tstart = device_data_queue[0]["time"]
        tend = device_data_queue[-1]["time"]

        matches = match_mass_transit_live(
            device, tstart, tend,
            MAX_MASS_TRANSIT_TIME_DIFFERENCE,
            MAX_MASS_TRANSIT_DISTANCE_DIFFERENCE,
            NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES).fetchall()

        print "d"+str(device), str(tstart)[:16], str(tend)[11:16], \
            str(len(device_data_queue))+"p:", ", ".join("%.2f %i %s" % (
                x[1], x[0], " ".join(x[2:])) for x in matches) \
            or "no nearby vehicles"

        hitreq = ((1.0*
                NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES
              - MAXIMUM_MASS_TRANSIT_MISSES)
          / NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES)

        if matches and matches[0]["hitrate"] >= hitreq:
            return matches[0]["line_type"], matches[0]["line_name"]

        return None, None


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
        
