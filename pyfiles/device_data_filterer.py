import json

from itertools import chain

from pyfiles.common_helpers import (
    get_distance_between_coordinates,
    pairwise,
    point_distance,
    point_interval,
    trace_center,
    trace_discard_inaccurate,
    trace_discard_sidesteps,
    trace_partition_movement)

from pyfiles.constants import (
    ACTIVITY_WIN,
    BAD_LOCATION_RADIUS,
    CONSECUTIVE_DIFFERENCE_LIMIT,
    DEST_DURATION_MIN,
    DEST_RADIUS_MAX,
    MAXIMUM_MASS_TRANSIT_MISSES,
    MAX_DIFFERENT_DEVICE_TIME_DIFFERENCE,
    MAX_MASS_TRANSIT_DISTANCE_DIFFERENCE,
    MAX_MASS_TRANSIT_TIME_DIFFERENCE,
    MAX_POINT_TIME_DIFFERENCE,
    NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES,
    STOP_BREAK_INTERVAL)

from pyfiles.database_interface import (
    device_data_filtered_table_insert,
    match_mass_transit_filtered,
    match_mass_transit_legs,
    match_mass_transit_live)

from pyfiles.mass_transit_match_planner import (
    find_same_journey_time_this_week, match_tripleg_with_publictransport,
    minSpeeds, TripMatchedWithPlannerResult)

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
    def _match_mass_transit(self, device_data_queue, activity, user_id):

        """Find mass transit match for leg in device_data_queue, using legacy
        filtered data, live vehicle locations, and trip planner."""

        # Use only relatively accurate points for mass transit matching, also
        # necessary so that matching previously recorded legs works...
        if device_data_queue and "accuracy" in device_data_queue[0]:
            device_data_queue = list(trace_discard_inaccurate(
                device_data_queue, DEST_RADIUS_MAX / 2))

        if len(device_data_queue) < 2:
            return {}

        # Reuse previously recorded matches in unchanged legs, useful when
        # reconstructing legs with changed algorithm or parameters.
        matches = self._match_mass_transit_legs(activity, device_data_queue)
        if matches is not None: # matched same start/end/activity
            print matches,
            return {x[0]: x[1:] for x in matches}

        if activity != "IN_VEHICLE":
            return {}

        matches = {}

        # If legacy filtered data exists, record a migrated match
        match = self._match_mass_transit_filtered(device_data_queue)
        if match not in (None, (None, None)):
            matches["FILTERED"] = match

        match = self._match_mass_transit_live(device_data_queue)
        if match not in (None, (None, None)):
            matches["LIVE"] = match

        # Needs user_id and prior match for csv/out spew
        match = self._match_mass_transit_planner(
            activity, device_data_queue, user_id, *(match or (None, None)))
        if match not in (None, (None, None)):
            matches["PLANNER"] = match

        return matches


    def generate_device_legs(self, points, start=None):
        """Generate sequence of stationary and moving segments of same activity
        from the raw trace of one device. Legs found in points before the start
        time, if given, are not emitted."""

        # Filter out bogus location points.
        points = trace_discard_sidesteps(points, BAD_LOCATION_RADIUS)

        lastpt = None

        # Ignore undecided points between segments
        partitioned = ((mov, seg) for (mov, seg) in trace_partition_movement(
                points,
                DEST_RADIUS_MAX,
                DEST_DURATION_MIN,
                STOP_BREAK_INTERVAL)
            if mov is not None)

        # Partition stationary and moving spans, with peekahead to next item
        for ms, nextms in pairwise(chain(partitioned, [(None, None)])):
            (mov, seg), (nextmov, nextseg) = (ms, nextms)

            # Emit stationary span, with rough centre as coordinate_start.
            if not mov:
                if len(seg) < 2:
                    continue

                lastpt = seg[-1]

                if start and seg[0]["time"] < start:
                    continue

                yield {
                    "time_start": seg[0]["time"],
                    "time_end": seg[-1]["time"],
                    "geojson_start": json.dumps({
                        "type": "Point",
                        "coordinates": trace_center(trace_discard_inaccurate(
                            seg, DEST_RADIUS_MAX / 2))}),
                    "activity": "STILL"}, {}
                continue

            # Join move segment to subsequent stop if it comes soon enough
            if nextmov is False and point_interval(
                    seg[-1], nextseg[0]) <= MAX_POINT_TIME_DIFFERENCE:
                seg.append(nextseg[0])

            # Feed moving span to activity stabilizer, mass transit detection.
            # This loses unstabilizable point spans.
            for legpts, legact in self._analyse_unfiltered_data(seg):

                # Inaccurate points can be useful at the activity detection
                # stage, but for mass transit matching and clustering, location
                # needs to be more accurate.
                legpts = list(trace_discard_inaccurate(
                    legpts, DEST_RADIUS_MAX / 2))

                if len(legpts) < 2:
                    continue # too few points to make a move, drop leg

                # Join to previous leg on shared point if close enough in time
                if lastpt and point_interval(
                        lastpt, legpts[0]) <= MAX_POINT_TIME_DIFFERENCE:
                    legpts.insert(0, lastpt)
                lastpt = legpts[-1]

                if start and legpts[0]["time"] < start:
                    continue

                km = .001 * sum(
                    point_distance(p0, p1) for p0, p1 in pairwise(legpts))

                leg = {
                    "time_start": legpts[0]["time"],
                    "time_end": legpts[-1]["time"],
                    "geojson_start": legpts[0]["geojson"],
                    "geojson_end": legpts[-1]["geojson"],
                    "activity": legact,
                    "km": km}

                yield leg, self._match_mass_transit(legpts, legact, None)


    def generate_filtered_data(self, device_data_rows, user_id):
        for legpts, legact in \
                self._analyse_unfiltered_data(device_data_rows, user_id):
            matches = self._match_mass_transit(legpts, legact, user_id)
            legtype, legname = matches.get("LIVE", (None, None))
            if not legtype:
                legtype, legname = matches.get("PLANNER", (None, None))
            self._write_filtered_data(
                legpts, legact, user_id, legtype, legname)


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

            line_type = line_name = None
            line_name_str = "None"

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
            
            # less than 200 meter, not a big impact! --> ignore            
            if distance > 200:
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
            if DUMP_CSV_FILES:
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


    def _match_mass_transit_legs(self, activity, device_data_queue):
        return match_mass_transit_legs(
            device_data_queue[0]["device_id"],
            device_data_queue[0]["time"],
            device_data_queue[-1]["time"],
            activity)


    def _match_mass_transit_filtered(self, device_data_queue):
        return match_mass_transit_filtered(
            device_data_queue[0]["device_id"],
            device_data_queue[0]["time"],
            device_data_queue[-1]["time"])


    def _match_mass_transit_live(self, device_data_queue):
        device = device_data_queue[0]["device_id"]
        tstart = device_data_queue[0]["time"]
        tend = device_data_queue[-1]["time"]

        matches = match_mass_transit_live(
            device, tstart, tend,
            MAX_MASS_TRANSIT_TIME_DIFFERENCE,
            MAX_MASS_TRANSIT_DISTANCE_DIFFERENCE,
            NUMBER_OF_MASS_TRANSIT_MATCH_SAMPLES)

        if matches is None:
            print "no vehicle data available"
            return None

        matches = matches.fetchall()

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


    def _analyse_unfiltered_data(self, device_data_rows, user_id=None):
        """Generate stabilized contiguous activity spans from raw trace."""

        if DUMP_CSV_FILES:
            self._dump_csv_file_open(user_id)

        rows = list(device_data_rows)
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
                    yield device_data_queue, chosen_activity
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
                yield device_data_queue[:-splitting_point], chosen_activity
                device_data_queue = device_data_queue[-splitting_point:]
                consecutive_differences = 0
                different_activity_counter = 0
                chosen_activity = current_activity

            previous_device_id = current_row["device_id"]
            previous_time = current_row["time"]
            previous_activity = current_activity

        if chosen_activity != "NOT_SET":
            yield device_data_queue, chosen_activity

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