
from sqlalchemy.sql import text
import json

from pyfiles.common_helpers import (http_request_with_get, pointRow_to_geoText, geoLoc_to_pointRow, jsonstr_to_sqlstr,  dict_to_sqlstr, shift_time_to_specific_date, 
                                                                OTPTimeStampToNormalDateTime, OTPDurationToNormalDuration,  DateTime_to_Text,  pointRow_to_postgisPoint)
from pyfiles.trip import(Trip)
from pyfiles.logger import(log, loge, logi)

import datetime
from datetime import datetime
from datetime import timedelta
from copy import deepcopy

PublicTransportModesTemplate = {'BUS', 'TRAM', 'RAIL', 'SUBWAY', 'FERRY'}
OTP_ERROR_CODE_DATE_TOO_FAR = 406
FIRST_LOOK_AT_TRIPLANS_TABLE = True
        
class TripPlanningResult:
    def __init__(self):
        self.error_code = 0
        self.error_msg = ''
        self.error_message = ''
        
    def to_Text(self):
        try:
            return str(self.error_code)+ ", "+ self.error_msg+ ", "+ self.error_message
        except Exception as e:
            print ">> TripPlanningResult.to_Text():: (!) EXCEPTION catched: ", e

class TripPlanner:
    def __init__(self,  db):
        self.db = db

    def request_a_trip_plan(self, desiredTrip, desiredTransportMode, numItineraries, maxWalkDistance, showIntermediateStops):
        log("")
        log(["request_a_trip_plan::Requesting to OTP journey planner:"])
        log(["Input desiredTrip:", desiredTrip.starttime, ",", desiredTransportMode])

        # query OTP journey planner ----------:
        # TODO: use Interface / Abstract class *:
        #   later planner match for the whole finland*: https://api.digitransit.fi/routing/v1/routers/finland/
        #   later plannermatch for all possible countries: ??? OTP API interfance	
        # ex: apiurl = IOTPServer.GetOTPAPIUrl() # shuld give the suitable instance, based on city/coutnry or user settings ...
        # NOTE:
        # example: querystr = "fromPlace=60.170718,24.930221&toPlace=60.250214,25.009566&date=2016/4/22&time=17:18:00&numItineraries=3&maxTransfers=3&maxWalkDistance=1500"

        apiurl = 'http://api.digitransit.fi/routing/v1/routers/hsl/plan'
        if desiredTransportMode == 'PUBLIC_TRANSPORT' or desiredTransportMode in PublicTransportModesTemplate:
            querystr = "fromPlace={0}&toPlace={1}&date={2}&time={3}&numItineraries={4}&maxWalkDistance={5}&showIntermediateStops={6}"\
			            .format(pointRow_to_geoText(desiredTrip.origin), pointRow_to_geoText(desiredTrip.destination), \
			                    datetime.date(desiredTrip.starttime), datetime.time(desiredTrip.starttime), \
			                    numItineraries, maxWalkDistance, showIntermediateStops)
        else:
            querystr = "fromPlace={0}&toPlace={1}&date={2}&time={3}&numItineraries={4}&mode={5}"\
			            .format(pointRow_to_geoText(desiredTrip.origin), pointRow_to_geoText(desiredTrip.destination), \
			                    datetime.date(desiredTrip.starttime), datetime.time(desiredTrip.starttime), \
			                    numItineraries, desiredTransportMode)
        json_data = http_request_with_get(apiurl, querystr)

        if 'plan' not in json_data or 'itineraries' not in json_data['plan']:
            if 'error' in json_data:
                return 0, None, json_data['error'], json_data
            else:
                return 0, None, '{"id":0, "msg":"", "message":""}', json_data
        else:
            return 1, json_data['plan'], None, json_data


    def plan_a_trip(self, reference_trip, desiredTransportMode):
        log("")
        log(["plan_a_trip::Input reference trip: ", reference_trip.starttime, "to", reference_trip.endtime, \
              ":  from ", pointRow_to_geoText(reference_trip.origin), "--> to", pointRow_to_geoText(reference_trip.destination)])

        desiredTrip = deepcopy(reference_trip) # keep a copy, in order to not change the input params in the caller's scope                
        desiredTrip.starttime = desiredTrip.starttime.replace(microsecond = 0)
        desiredTrip.endtime = desiredTrip.endtime.replace(microsecond = 0)

        # query journey planner ----------:
        # assumptions, constants, adjusting parameters, related cals, some kinematics, etc. -----------------:
        MAX_MODE_DETECTION_DELAY = 500 # (meters) we have latency in making sure of the mode change
#        MAX_GPS_ERROR = 50 # (in meters) if error larger than this, we've discarded that point TODO ???
#        MAX_VEHICLE_LENGTH = 50 # TODO
#        MAX_DISTANCE_FOR_POINT_MATCH = MAX_GPS_ERROR + MAX_VEHICLE_LENGTH
        #MAX_GPS_ERROR = 1000 # meters (somehow maxWalkDistance is equal to GPS error threshold for our system)
                            # or maybe not... this could be also max_distance_between_busstops / 2 !!  (if we have a detection between two bus stops) 
                            # 1000 m (e.g. 500 m walkking at each trip end) gives good results for user id 13 
        maxWalkDistance = MAX_MODE_DETECTION_DELAY * 2 # e.g. 500m walk to start bus stop ... 500m walk to end bus stop
        numItineraries = 3 # default is 3
        maxTransfers = 2  # seems like this param didn't have any effect!
        showIntermediateStops = "True"
        # TODO: is there a param 'max waiting time' too?                
        

        desiredTrip_datetime_shifted = False        
        desiredTripDateShifted = None
        
        tripplan_made_before = False                
        if FIRST_LOOK_AT_TRIPLANS_TABLE:
            #TODO important, later, possible BUG: if 'itin_starttime' (planned) differs a bit from the stored 'desiredTrip.starttime' (desired) ... some plans may get lost ??!
            # one solution: use the requestedParameters['date'] and ['time'] instead of values from itin ???
            res, plan, error, planning_response = self.find_trip_plan_already_made(desiredTrip, desiredTransportMode, numItineraries, maxWalkDistance)                
            
            if res == 0 and error['id'] == OTP_ERROR_CODE_DATE_TOO_FAR: # second try (adjust the old weekday to current week)
                loge(["trip", reference_trip.user_id,  reference_trip.id,"-", desiredTransportMode,": plan_a_trip:: load from DB table FAILED because: OTP_ERROR_CODE_DATE_TOO_FAR! => Trying second time with the shifted date (either 'trips.shifted_starttime_for_publictransport_tripplan' or time current week) ..."])
                desiredTrip_datetime_shifted = True # ***
                desiredTripDateShifted = deepcopy(desiredTrip)
                if desiredTrip.shifted_starttime_for_publictransport_tripplan is not None: # if already shifted in a previous run, load from DB record values
                    desiredTripDateShifted.starttime = desiredTrip.shifted_starttime_for_publictransport_tripplan
                else: # if not already shifted, do it here, try for same weekday in current week 
                    desiredTripDateShifted.starttime = self.find_same_journey_time_this_week(desiredTrip.starttime)
                desiredTripDateShifted.endtime = shift_time_to_specific_date(desiredTrip.endtime, desiredTripDateShifted.starttime) #TODO ... no separate data field for endtime ?!!! 
                res, plan, error, planning_response = self.find_trip_plan_already_made(desiredTripDateShifted, desiredTransportMode, numItineraries, maxWalkDistance)                    

            tripplan_made_before = (res == 1)
            
        if not tripplan_made_before: # if not loaded from previous plans, send trip-planning query:
            res, plan, error, planning_response = self.request_a_trip_plan(desiredTrip, desiredTransportMode, numItineraries, maxWalkDistance, showIntermediateStops)        
            
            if res == 0 and error['id'] == OTP_ERROR_CODE_DATE_TOO_FAR: # second try (adjust the old weekday to current week)
                loge(["trip", reference_trip.user_id,  reference_trip.id, "-", desiredTransportMode, ": plan_a_trip:: FAILED because: OTP_ERROR_CODE_DATE_TOO_FAR! => Trying second time with current week..."])
                self.store_trip_plan(desiredTrip, desiredTransportMode, numItineraries, maxWalkDistance,  planning_response) #store this 'intermediate' trip-plan response for later use ***                
                desiredTrip_datetime_shifted = True # ***
                desiredTripDateShifted = deepcopy(desiredTrip)            
                desiredTripDateShifted.starttime = self.find_same_journey_time_this_week(desiredTrip.starttime)
                desiredTripDateShifted.endtime = self.find_same_journey_time_this_week(desiredTrip.endtime)
                res, plan, error, planning_response = self.request_a_trip_plan(desiredTripDateShifted, desiredTransportMode, numItineraries, maxWalkDistance, showIntermediateStops) 
                if res == 1: 
                    self.store_trip_plan(desiredTripDateShifted, desiredTransportMode, numItineraries, maxWalkDistance,  planning_response) #store this 'shifted' trip-plan-response for later use ***
            elif res == 1: 
                    self.store_trip_plan(desiredTrip, desiredTransportMode, numItineraries, maxWalkDistance,  planning_response) #store this trip-plan-response for later use ***

        # update the original plan field (later in code trips rows are stored in DB):
        if desiredTrip_datetime_shifted: 
            reference_trip.shifted_starttime_for_publictransport_tripplan = desiredTripDateShifted.starttime
        
        #    
        trip_planning_res = None
        if res == 0:
            trip_planning_res = TripPlanningResult()        
            if error is not None:
                trip_planning_res.error_code = error['id']
                trip_planning_res.error_msg = error['msg']
                trip_planning_res.error_message =  error['message']
                return 0, None, trip_planning_res
            else:
                return 0, None, trip_planning_res
        
        # go through all trip-leg-chains suggested ------ :   
        log("")
        log(["Working on the itins (routes) suggested by otp journey planner ...:"])
        itin_index = 0
        matchcount = 0
        plannedmatches = [] # TODO: choose which returned trip? e.g. when 3 public transport trips are returned * order based on what?
        trips = []
        
        for itin in plan['itineraries']: 
            trip = Trip()
            trip.user_id = desiredTrip.user_id
            trip.device_id = desiredTrip.device_id
            trip.id = desiredTrip.id            
            itin_starttime = OTPTimeStampToNormalDateTime(itin['startTime'])
            itin_endtime = OTPTimeStampToNormalDateTime(itin['endTime'])
            # trip.shifted_starttime_for_publictransport_tripplan = itin_starttime # no need to use this field for alternative plans? (plan_id > 0)
            if desiredTrip_datetime_shifted:
                trip.starttime = shift_time_to_specific_date(itin_starttime,  desiredTrip.starttime) # TODO: NOTE: starttime of plan may differ from desired trip start-time (???)
                trip.endtime = shift_time_to_specific_date(itin_endtime,  desiredTrip.endtime)
            else: 
                trip.starttime = itin_starttime # TODO: NOTE: starttime.time()/endtime of planned itinerary may differ *a bit* from desired trip start-time.time/endtime (???)
                trip.endtime = itin_endtime
            trip.origin = geoLoc_to_pointRow(plan['from']['lat'], plan['from']['lon']) # TODO: are there cases where plan's origin{lat,lon} differ a bit from desired trip origin?!
            trip.destination = geoLoc_to_pointRow(plan['to']['lat'], plan['to']['lon']) # TODO: are there cases where plan's destination{lat,lon} differ a bit from desired trip destination?!
            # trip.legs = itin['legs'] #TODO remove old code?
            trip.append_otp_legs(itin['legs'],  desiredTrip_datetime_shifted, desiredTrip.starttime,  desiredTrip.endtime)
            trips.append(trip)
            
        return 1, trips, None


    def find_same_journey_time_this_week(self, original_date_time):
        # find date of the same weekday, but for current week (to avoid querying old dates that results in error from HSL)
        date_thisweek = datetime.today() + timedelta(days = (original_date_time.weekday() - datetime.today().weekday()))
        time_thisweek = datetime.combine(date_thisweek.date(), original_date_time.time())
        log(["same_journey_time_this_week :: ", time_thisweek])
        return time_thisweek

    def store_trip_plan(self, trip, mode, numItineraries, maxWalkDistance,  plan):
        qstr = """INSERT INTO trip_plans (start_time, origin, destination, mode, max_walk_distance, no_of_itins, plan) 
                    VALUES ('{0}', 
                    ST_GeomFromText('{1}'), ST_GeomFromText('{2}'), 
                    '{3}',{4},{5},
                    '{6}' ); """.format(
                    DateTime_to_Text(trip.starttime), 
                    pointRow_to_postgisPoint(trip.origin), pointRow_to_postgisPoint(trip.destination), 
                    mode, maxWalkDistance, numItineraries, 
                    jsonstr_to_sqlstr(json.dumps(plan))
                    )
        
        log(["store_trip_plan():: qstr:", qstr])
        log("")
                
        res = ""
        try:
            res = self.db.engine.execute(text(qstr))
        except Exception as e:
            print ">> store_trip_plan():: FAILED ------------------------------"
            print ">> plan:",  plan
            print ".............................................."
            print ">> qstr:",  qstr
            print ".............................................."            
            print ">> (!) EXCEPTION catched: ", e
            print ""
            print ""
        
        log(["result:", str(res)])
        
                
    def find_trip_plan_already_made(self, trip, mode, numItineraries, maxWalkDistance):
        qstr = """SELECT start_time, 
                                ST_AsText(origin) as origin, ST_AsText(destination) as destination, 
                                mode, max_walk_distance, no_of_itins, 
                                plan 
                            FROM trip_plans  
                            WHERE start_time = '{0}' AND 
                                origin = '{1}' AND destination = '{2}' AND
                                mode = '{3}' AND max_walk_distance = {4} AND no_of_itins = {5};
                            """.format(
                                DateTime_to_Text(trip.starttime),  
                                pointRow_to_postgisPoint(trip.origin), pointRow_to_postgisPoint(trip.destination), 
                                mode,  maxWalkDistance,  numItineraries
                            )
                    
        log(["find_trip_plan_already_made():: qstr:", qstr])
        log("")
        
        plan_rows =  self.db.engine.execute(text(qstr))   
        
        if plan_rows.rowcount > 0:
            for plan_row in plan_rows:
                return self.parse_planning_response(json.loads(plan_row['plan']))                
        else:
            return 0,  None, json.loads('{"id":0, "msg":"", "message":"Such trip plan not stored before"}'), None
            
    def parse_planning_response(self, result_data_dict):
        if 'plan' not in result_data_dict or 'itineraries' not in result_data_dict['plan']:
            #itineraries_count = len (result_data_dict['plan']['itineraries'])
            #if itineraries_count == 0:
            # print "journey planner did NOT return any itineraries!\n"
            # print "result_data_dict returned:\n", result_data_dict, "\n"
            # print "result_data_dict error section:\n", result_data_dict['error']                        
            if 'error' in result_data_dict:
                return 0, None, result_data_dict['error'], result_data_dict
            else:
                return 0, None, json.loads('{"id":0, "msg":"", "message":""}'), result_data_dict
        else:
            return 1, result_data_dict['plan'], None, result_data_dict                
