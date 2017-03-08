import json

from pyfiles.common_helpers import (point_distance, geoJSON_to_pointRow, OTPTimeStampToNormalDateTime, OTPDurationToNormalDuration, shift_time_to_specific_date)
from pyfiles.user import (User)
from pyfiles.logger import(log, loge, logi)
from copy import (deepcopy)
from datetime import timedelta

class Trip:
    def __init__(self, trip_row = None):
        if trip_row is None:
            self.user_id = None
            self.device_id = None                
            self.id = None
            self.plan_id = None
            
            self.origin = None # NOTE: of type TS 'point', but only one field: {"geojson": <GeoJSON of the point>}
            self.destination = None # NOTE: of type TS 'point', but only one field: {"geojson": <GeoJSON of the point>}
            self.starttime = None
            self.endtime = None       

            self.legs = [] # collection of type 'legs' table row

            # trip economics values:
            self.distance = None
            self.duration = None
            self.cost = None
            self.calories = None
            self.emission = None
            self.comfort = None
            self.distance_by_mode = {}                        
            self.duration_by_mode = {}                
            self.cost_by_mode = {}
            self.calories_by_mode ={}
            self.emission_by_mode ={}
            
            self.alternative_trips = [] # a collection of type:class Trip
            self.user = None # TODO is this used now?!
            self.has_transitLeg = False
            
            self.multimodal_summary = None
            self.mainmode = None        
            
            self.shifted_starttime_for_publictransport_tripplan = None
            self.notes = None 
        else:
            self.user_id = trip_row['user_id']
            self.device_id = trip_row['device_id']                
            self.id = trip_row['id']
            self.plan_id = trip_row['plan_id']
            
            self.origin = geoJSON_to_pointRow(trip_row['origin'])
            self.destination = geoJSON_to_pointRow(trip_row['destination'])
            self.starttime = trip_row['start_time']
            self.endtime = trip_row['end_time']

            self.legs = [] # collection of type 'legs' table row

            # trip economics values:
            self.distance = trip_row['distance']
            self.duration = trip_row['duration']
            self.cost = trip_row['cost']
            self.calories = trip_row['calories']
            self.emission = trip_row['emission']
            self.comfort = trip_row['comfort']
            # TODO following !!!
            self.distance_by_mode = json.loads(trip_row['distance_by_mode'])
            self.duration_by_mode = json.loads(trip_row['time_by_mode'].replace("'", '"'))
            self.cost_by_mode = json.loads(trip_row['cost_by_mode'].replace("'", '"'))
            self.calories_by_mode =json.loads(trip_row['calories_by_mode'].replace("'", '"'))
            self.emission_by_mode =json.loads(trip_row['emission_by_mode'].replace("'", '"'))
            
            self.alternative_trips = [] # a collection of type:class Trip
            self.user = None # TODO is this used now?!
            self.has_transitLeg = False # TODO, what to set?!!
            
            self.multimodal_summary = trip_row['multimodal_summary']
            self.mainmode = trip_row['mainmode']        
            
            self.shifted_starttime_for_publictransport_tripplan = trip_row['start_time_for_plan']
            self.notes = trip_row['notes']        
                 
        
    def append_otp_legs(self, otpLegs, desiredTrip_datetime_shifted, desired_date_start,  desired_date_end):
        for leg in otpLegs:
            self.append_otp_leg(leg,  desiredTrip_datetime_shifted, desired_date_start,  desired_date_end)
    
    def append_otp_leg(self, otpLeg, desiredTrip_datetime_shifted, desired_date_start,  desired_date_end): # adding legs retrieved from OTP
        leg = deepcopy(otpLeg)

        leg_starttime = OTPTimeStampToNormalDateTime(leg['startTime'])
        leg_endtime = OTPTimeStampToNormalDateTime(leg['endTime'])        
        # leg['time_start_for_plan'] = leg_starttime # TODO ? no need for this field?
        # leg['time_end_for_plan'] = leg_endtime    # TODO ? no need for this field?                
        if desiredTrip_datetime_shifted:
            leg['time_start'] = shift_time_to_specific_date(leg_starttime,  desired_date_start) # TODO: NOTE: starttime of plan may differ from desired trip start-time (???)
            leg['time_end'] = shift_time_to_specific_date(leg_endtime,  desired_date_end)
        else:
            leg['time_start'] = leg_starttime
            leg['time_end'] = leg_endtime
        # TODO IMPORTANT!!! - do this also for details:
        #leg['from']['arrival']
        #leg['from']['departure']
        #leg['to']['arrival']
        #leg['to']['departure']
        #   leg['intermediateStops'][i]['arrival]
        #   leg['intermediateStops'][i]['departure]
        
        leg['duration'] = OTPDurationToNormalDuration(leg['duration'])
        if leg['transitLeg'] == True:
            self.has_transitLeg = True # NOTE: TODO: not needed anymore?!
        
            
        self.legs.append(leg)
    
    def append_trafficsense_leg(self, legRow): # adding legs detected by TS from actual trips
        leg = dict(legRow.items())
        # NOTE: leg['origin'] and leg['destination'] are of type GeoJSON
        leg['distance'] = point_distance(geoJSON_to_pointRow(leg['origin']), geoJSON_to_pointRow(leg['destination'])) 
        leg['mode'] = 'NOT_CONVERTED' #TODO ...
        leg['transitLeg'] = False # NOTE: important field assignment       
        leg['alerts'] = None # Note: ... used later to estimate 'comfort' param 
        
        if leg['activity'] == 'IN_VEHICLE' and leg['line_type'] is None: #driving
            leg['mode'] = 'CAR'
        elif leg['activity'] == 'IN_VEHICLE' and leg['line_type'] is not None: #public transport
            leg['mode'] = leg['line_type']
            leg['transitLeg'] = True # NOTE: important field assignment
            self.has_transitLeg = True # NOTE: TODO: not needed anymore?!
            if leg['mode'] == 'TRAIN':
                leg['mode'] = 'RAIL'            
        elif leg['activity'] == 'WALKING':
            leg['mode'] = 'WALK'
        elif leg['activity'] == 'RUNNING':
            leg['mode'] = 'RUN'
        elif leg['activity'] == 'ON_BICYCLE':
            leg['mode'] = 'BICYCLE'
        #TODO add condition for eBike
        
        self.legs.append(leg)        
        # print "appended TS leg:", leg            

    def add_duration(self, mode, duration):
        if mode not in self.duration_by_mode:
            self.duration_by_mode[mode] = timedelta(0)
        self.duration_by_mode[mode] += duration
        #self.duration += duration # TODO: now trip's whole duration is calculated at once
        
    def add_cost(self, mode, cost):
        if mode not in self.cost_by_mode:
            self.cost_by_mode[mode] = 0    
        self.cost_by_mode[mode] += cost
        self.cost += cost
        
    def add_calories(self, mode, cals):
        if mode not in self.calories_by_mode:
            self.calories_by_mode[mode] = 0    
        self.calories_by_mode[mode] += cals
        self.calories += cals
        
    def add_emission(self, mode, emission):
        if mode not in self.emission_by_mode:
            self.emission_by_mode[mode] = 0    
        self.emission_by_mode[mode] += emission
        self.emission += emission

    def add_travelled_distance(self, mode, distance):
        if mode not in self.distance_by_mode:
            self.distance_by_mode[mode] = 0    
        self.distance_by_mode[mode] += distance
        self.distance += distance
    
    def get_distance_by_mode(self, mode):
        if mode not in self.distance_by_mode:
            self.distance_by_mode[mode] = 0    
        return self.distance_by_mode[mode]

