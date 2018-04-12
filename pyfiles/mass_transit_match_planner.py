import datetime
from datetime import datetime
from datetime import timedelta

import json
#import os
#import sys
import time
#import re

# mehrdad - new
#import urllib
import requests
import urllib.request, urllib.error, urllib.parse
import polyline

from pyfiles.common_helpers import (get_distance_between_coordinates, point_distance, point_coordinates)

# generate extensive logs!
LOG_DETAILS = True

def HttpRequestWithGet(apiurl, querystr):
	# Notes: 
	# GET request with urllib2: If you do not pass the data argument, urllib2 uses a GET request (you can encode the data in URL itself)
	# POST request with urllib2: Just as easy as GET, simply passing (encoded) request data as an extra parameter
	
	#params = urllib.urlencode(querystr)
	params = querystr
	print("params: ", params)

	apiurl_with_query = apiurl + "?" + params;
	print("apiurl_with_query: ", apiurl_with_query)
	
	json_data = '{}'
	try:
		#response = urllib2.urlopen(apiurl_with_query)
		response = requests.get(apiurl_with_query, verify=False) # //TODO: possible security issue! verify=True works from python command line !!!!
		#print response.text
		json_data = json.loads(response.content)
		#print "json_data:", json_data
		response.close()
	except urllib.error.HTTPError as e:
		print("(!) EXCEPTION catched: ", e)
	except requests.exceptions.ConnectionError as e:
		print("(!) EXCEPTION catched: ", e)
	except Exception as e:
		print("(!) EXCEPTION catched: ", e)

	return json_data

# ------ date time and duration helper functions -------
def HSLTimeStampToNormalDateTime(hsl_timestamp):
    date_and_time = datetime.fromtimestamp(hsl_timestamp / 1000) #datetime doesn't like millisecond accuracy
    return date_and_time

def HSLDurationToNormalDuration(hsl_duration):
    # hsl_duration is in seconds
    duration = timedelta(seconds = hsl_duration)
    # old code (converetd to string!): duration = "{0}:{1}".format(hsl_duration/60 , hsl_duration%60)  # convert to in min:sec format
    return duration


# -------- matching trip-legs with results from journey planner --------------------------

def find_same_journey_time_this_week(starttime):
    # find date of the same weekday, but for current week (to avoid querying old dates that results in error from HSL)
    date_thisweek = datetime.today() + timedelta(days = (starttime.weekday() - datetime.today().weekday()))
    time_thisweek = datetime.combine(date_thisweek.date(), starttime.time())
    print("same_journey_time_this_week :: ", time_thisweek)
    return time_thisweek

class PlannedTrip:
    start = None    # for whole trip (not specific leg)
    end = None      # for whole trip (not specific leg)
    # for the chosen leg (the transit leg):
    linetype = None
    linename = None       
    legstart = None
    legend = None 
    deltaT = None
    deltaTsign = None
    deltaStarttime = None
    deltaStarttimeStr = None
    deltaStartPassed = None
    matchedbytime = None
    matchedbyroute = None
    matched_fraction = None
    longest_serialunmatch = None

class TripMatchedWithPlannerResult:
    trip = None # its type should be PlannedTrip 
    matchcount = 0
    bestmatchindex = 0
 

minSpeeds = {"walk":1.34112, "bus": 3.0, "tram":2.5, "train":5.0, "ferry":5.0} # walk speed default: 3 MPH (1.34112 m/s) (~ 5.0 km/h)


def match_tripleg_with_publictransport(fromPlace, toPlace, trip_starttime, trip_endtime, all_recorded_trip_points):
    print("Input Trip ...:")
    print("trip_starttime:", trip_starttime)
    print("trip_endtime:", trip_endtime)

    tripmatchres = TripMatchedWithPlannerResult()

    # TODO TODO, who about these paratms returned by journey planner? 
    # <walkTime>394</walkTime>
    # <transitTime>1020</transitTime>
    # <waitingTime>442</waitingTime>
    # <walkDistance>489.6903846194335</walkDistance>
    # <walkLimitExceeded>false</walkLimitExceeded>
    # <elevationLost>0.0</elevationLost>
    # <elevationGained>0.0</elevationGained>
    # <transfers>1</transfers>
    # returned by planner: 
    #   <tooSloped>false</tooSloped>
    
    recordedpoints = all_recorded_trip_points
    trip_starttime = trip_starttime.replace(microsecond = 0)
    trip_endtime = trip_endtime.replace(microsecond = 0)
    trip_duration = trip_endtime - trip_starttime

    # assumptions, constants, adjusting parameters, related cals, some kinematics, etc. -----------------:
    MAX_MODE_DETECTION_DELAY = 500 # (meters) we have latency in making sure of the mode change
    MAX_GPS_ERROR = 50 # (in meters) if error marger than this, we've discarded that point TODO ???
    MAX_VEHICLE_LENGTH = 50 # TODO
    MAX_DISTANCE_FOR_POINT_MATCH = MAX_GPS_ERROR + MAX_VEHICLE_LENGTH
    #MAX_GPS_ERROR = 1000 # meters (somehow maxWalkDistance is equal to GPS error threshold for our system)
                        # or maybe not... this could be also max_distance_between_busstops / 2 !!  (if we have a detection between two bus stops) 
                        # 1000 m (e.g. 500 m walkking at each trip end) gives good results for user id 13 
    maxWalkDistance = MAX_MODE_DETECTION_DELAY * 2 # e.g. 500m walk to start bus stop ... 500m walk to end bus stop
    numItineraries = 3 # default is 3
    maxTransfers = 2  # seems like this param didn't have any effect!
    showIntermediateStops = "True"
    # TODO: is there a param 'max waiting time' too?

    maxIntervals = {"bus":60, "tram":30, "train":60, "ferry":60} # max arrival interval of each public transport mode during working hours (minutes)
    maxSlowness = {"bus":3, "tram":3, "train":3, "ferry":5} # maximum slowness (a bit different concept than 'being late') of public transport (minutes)
                                                            # we should note that being "slower"/"faster" is different than bus arrival being "late"/"early"
                                                            # TODO! depends also on the city! in Helsinki it's sharp! :) ... in Rome, maybe not
    maxDError = MAX_MODE_DETECTION_DELAY * 2 # maximum Distance error (e.g: one deltaD at each end of the trip)
    
    legstartshift = timedelta(seconds = round(MAX_MODE_DETECTION_DELAY/minSpeeds['walk']))  # default: 4 minutes (*) or CALC: e.g: MAX_MODE_DETECTION_DELAY/minSpeeds['walk']        
    trip_starttime_earlier = trip_starttime - legstartshift
    print("legstartshift:",legstartshift)
    print("trip_starttime_earlier:", trip_starttime_earlier , " (note: WE'LL GIVE THIS TO JOURNEY PLANNER QUERY *)")
    print("")
    
    # query journey planner ----------:
    print("Query to journey planner:")
    # TODO: use Interface / Abstract class *:
    #   later planner match for the whole finland*: https://api.digitransit.fi/routing/v1/routers/finland/
    #   later plannermatch for all possible countries: ??? OTP API interfance	
    # ex: apiurl = IOTPServer.GetOTPAPIUrl() # shuld give the suitable instance, based on city/coutnry or user settings ...
    apiurl = 'http://api.digitransit.fi/routing/v1/routers/hsl/plan'
    querystr = "fromPlace={0}&toPlace={1}&date={2}&time={3}&numItineraries={4}&maxWalkDistance={5}&showIntermediateStops={6}"\
			    .format(fromPlace, toPlace, datetime.date(trip_starttime_earlier), datetime.time(trip_starttime_earlier), \
			            numItineraries, maxWalkDistance, showIntermediateStops);
    #ex: querystr = "fromPlace=60.170718,24.930221&toPlace=60.250214,25.009566&date=2016/4/22&time=17:18:00&numItineraries=3&maxTransfers=3&maxWalkDistance=1500" # ******
    json_data = HttpRequestWithGet(apiurl, querystr)

    if 'plan' not in json_data or 'itineraries' not in json_data['plan']:
        #itineraries_count = len (json_data['plan']['itineraries'])
        #if itineraries_count == 0:
        # print "journey planner did NOT return any itineraries!\n"
        # print "json_data returned:\n", json_data, "\n"
        # print "json_data error section:\n", json_data['error']
        
        if 'error' in json_data:
            return json_data['error']['id'], tripmatchres
        else:
            return 0, tripmatchres

    print("")
    print("Working on the routes suggested by journey planner ...:")
    itin_index = 0
    matchcount = 0
    plannedmatches = []
    
    # go through all routes suggested ------ : 
    for itin in json_data['plan']['itineraries']: 
        duration = HSLDurationToNormalDuration(itin['duration'])        # duration of planned trip
        starttime = HSLTimeStampToNormalDateTime(itin['startTime'])    # start time of planned trip
        endtime = HSLTimeStampToNormalDateTime(itin['endTime'])         # ...
        print("\n#", itin_index+1, ": This journey planner trip's duration, start & ends time: ", \
                duration,"(",starttime , "-->", endtime,")\t")#, itin['duration'], " seconds", "(",itin['startTime'], itin['endTime'],")" 

        # prepare the adjuster params TODO (instead of 'bus', it could be 'train', 'tram' ... depending on planned results) *
        maxdeltaW = timedelta(seconds = round(maxDError/minSpeeds['walk']))   # max acceptable diff, result of walking to the stop        
        maxdeltaB = timedelta(seconds = round(maxDError/minSpeeds['bus']))    # max acceptable diff, result of bus traveling 1 stop less or more
        maxdeltaT = maxdeltaW + maxdeltaB
        maxI = timedelta(minutes = maxIntervals['bus']) # in minutes
        maxdeltaSlowness = timedelta(minutes = maxSlowness['bus'])    
        # maxdeltaSlowness = timedelta(minutes = 0) # TODO !!!! temp. , remove this 
        print("maxdeltaW, maxdeltaB, maxdeltaT, maxI: ", maxdeltaW, maxdeltaB, maxdeltaT, maxI, "maxdeltaB/2 + maxdeltaSlowness:",maxdeltaB/2 + maxdeltaSlowness)

        deltaT = abs(duration - trip_duration)
        deltaTsign = ""
        if duration < trip_duration:
            deltaTsign = "shorter"

        legsmatched = 0        
        legsreviewed = False # TODO: mostly for debugging
        # COND: pattern of planned trip-legs, 
        #   for example usually the idea is: 'WALK', <ride>, 'WALK' ... or WALK, RIDE (no ending walk)
        #   there should not be more than 1 'ride' (mass_transit invehicle) trip-leg
        transitlegs_count = 0
        for leg in itin['legs']:
            if leg['transitLeg']:
                transitlegs_count += 1                
        if transitlegs_count != 1: #TODO: support multi-leg match later asap! *
            print("number of planned transit legs:", transitlegs_count, " (should be 1) => ignoring this journey planner trip (!)")                         
        # COND: compare original trip's total duration WITH planned trip total duration
        elif duration < trip_duration and deltaT > maxdeltaSlowness: #TODO: ideally, maxdeltaSlowness should depend on current mode of transport        
            print("original trip_duration:",trip_duration, "planned trip duration:", duration, "duration < trip_duration", " (deltaT:- ", deltaT,")", \
                    "=> how come planned trip is this much shorter than original trip ??!! => ignoring this journey planner trip (!)") # one reason could be: maybe the bus or tram ran faster this time!
        elif deltaT > maxdeltaT:
            print("original trip_duration:",trip_duration, "planned trip duration:", duration, "(deltaT:",deltaT,"maxdeltaT:",maxdeltaT,")", \
                    "=> too much difference! ignoring this journey planner trip (!)")
        else: 
        # now look at legs of this itin to maybe find a match *
            legsreviewed = True
            print("Legs > > > > > > : ")
            planlegs = itin['legs']
            legindex = 0
            for leg in planlegs:
                matchedbytime = False
                matchedbyroute = False
                    
                mode = leg['mode']
                line = leg['route']
                istransit = leg['transitLeg']
                legduration = leg['duration']
                legdurationnormal = HSLDurationToNormalDuration(int(legduration)) # TODO why int() ?
                legstarttime = HSLTimeStampToNormalDateTime(leg['startTime'])
                legendtime = HSLTimeStampToNormalDateTime(leg['endTime'])

                istransitstr = ""
                if istransit: 
                    istransitstr = " ***"

                # matching logic * (TODO >> also refer to paper notes, math equations, kinematics, assumptions, etc.) :
                #   unavoidable delay in detections: our recorded point might be in between two bus stops (after the original origin bus stop and before 2nd or 3rd bus stop)
                #   --> assume: person started the trip 1-2 minutes earlier
                #               person started the trip ~500 meters before               
                ridematched_str = ""                
                if istransit:                
                    # COND: compare original-trip total duration WITH duration of planned transit-leg (bus, tram, etc)
                    deltaTransitLeg = abs(trip_duration - legdurationnormal)

                    if deltaTransitLeg <= maxdeltaB:                
                        # COND: planned start time not too far, e.g.: not more than bus interval
                        deltaStarttime = abs(legstarttime - trip_starttime)
                        deltaStartPassed = True
                        if deltaStarttime > (maxdeltaB/2 + maxdeltaSlowness):
                            deltaStartPassed = False
                            
                        if deltaStarttime > maxI:
                            deltaStartPassed = False
                        else:
                            # matched with witch line_type? TRAM, SUBWAY, BUS, ...?
                            matchedbytime = True    # this leg is a match time-based *                                             
                            legsmatched += 1
                            ridematched_str = ":::::: this leg might be a match (time-based)!!"                                                                    
                                                   
                        if not deltaStartPassed:
                            print("planned trip-leg starts too late => ignoring this leg !!!")                                                             
                #END if is_transit

                #line name encoding, now considered utf-8 #TODO
                line_name_str = "None"
                if line: 
                    line_name_str = line.encode('utf-8')                
                print(mode, line_name_str, ", is transit:", istransit, "| Duration: ", \
                        legdurationnormal, "(",legstarttime,"-->",legendtime,")", istransitstr, ridematched_str)

                matched_fraction = 0      
                longest_serialunmatch = 0              
                if matchedbytime and deltaStartPassed: # if this leg is matched (time-based), then  # TODO refactor conditions
                    #now check location-based
                    print("")
                    print("trying to match location-based as well ...")
                    # TODO NOTICE!
                    #   there are so many bus and trams in city that for every car-ride there can be a similar bus/tram ride
                    #   how to solve this without live data ??!!
                    #   - with following filters? ex: avg-speed values
                    #   - matching recorded points "time" with intermediate-stop points "time"?
                    # TODO 
                    #   - give match score/priority to each matched leg                                          
                    # TODO
                    #   get geometry points from planned leg
                    #   match with some intermediate points of the filtered trip-leg we have here
                    #   ? how many point matches are enough?                    

                    # recordedpoint_startindex = ... where deltastarttime ... => no. of skiprecordedpoints
                    # recordedpoint_endindex = ... ignore the last ? N ?  recorde points
                    # step = (166)/(8,610/200)
                    # step = (len(recordedpoints)) / (triplength_in_meters / 200)
                    # 
                    # go to indexes according to step (plus always incluide the last point) (OR exclude the last point!!!)
                    # exlude the points in the 'last minute'?
                    #
                    # do the matching loop
                    #   if N% of points do not have a match in plan ==> matching FAILED             ?
                    #   if M consecutive points do not have a match in plan ==> matching FAILED     ?              
                    #   or mayve check both conditions ,, with OR
                    
                    #
                    plannedpoints = polyline.decode(leg['legGeometry']['points'])                                                 
                                        
                    # extract the recorded points which most probably are only the transit part ("good points") (B' section in paper notes)
                    MIN_NUMBER_OF_GOODPOINTS = 4 # TODO ***
                    print("Extracting the goodpoints from recorded points ...")
                    prewalktime = timedelta(seconds=0)
                    postwalktime = timedelta(seconds=0)                   
                    if legindex>0:
                        if planlegs[legindex-1]['mode'] == 'WALK':
                            prewalktime = timedelta(seconds = planlegs[legindex-1]['duration'])
                    prewalktime_to_ridetime = timedelta(seconds = planlegs[legindex-1]['distance']/minSpeeds['bus'])                                        
                    if legindex<len(planlegs)-1:
                        if planlegs[legindex+1]['mode'] == 'WALK':
                            postwalktime = timedelta(seconds = planlegs[legindex+1]['duration'])                         
                    print("prewalktime:", prewalktime, "prewalktime_to_ridetime:", prewalktime_to_ridetime, "postwalktime:", postwalktime)
                    
                    print("recorded points (n=",len(recordedpoints),"):")
                    goodpoints = []  
                    hasgoodpoints = False
                    start_recordedpoint = recordedpoints[0]                                                              
                    end_recordedpoint = recordedpoints[len(recordedpoints)-1]
                    recordedtrip_starttime = start_recordedpoint['time']
                    recordedtrip_endtime = end_recordedpoint['time']
                    
                    for point in recordedpoints:
                        point_location = json.loads(point["geojson"])["coordinates"]
                        point_location_str='{1},{0}'.format(point_location[0],point_location[1])
                        point_time  = point['time']                        
                        isgoodpoint = "---"

                        # save this point only if its record time is within the range                        
                        # and also consider enough distance between points
                        moved = 0
                        n = len(goodpoints)
                        if n > 0:
                            previous_goodpoint = goodpoints[n-1]
                            moved = point_distance(point, previous_goodpoint)                        
                        if (point_time - recordedtrip_starttime) >= prewalktime_to_ridetime and (recordedtrip_endtime - point_time) >= postwalktime:
                            if n > 0:
                                if moved >= MAX_DISTANCE_FOR_POINT_MATCH: #TODO or after min 200 meters?
                                    goodpoints.append(point)                                                
                                    isgoodpoint = ""
                            else:# first detected good point:
                                goodpoints.append(point)                                                
                                isgoodpoint = ""
                                
                        print(point_location_str, point_time, isgoodpoint, moved, "(m) moved since last goodpoint")
                    #END loop ..........

                    if len(goodpoints) >= MIN_NUMBER_OF_GOODPOINTS:
                        hasgoodpoints = True
                    else:
                        print("not enough number of goodpoints extracted (no. of goodpoints=",len(goodpoints),"<",MIN_NUMBER_OF_GOODPOINTS,") !!!!!!")                        

                                                
                    # COND 2 -------------------:
                    if hasgoodpoints:
                        goodtripdistance = point_distance(goodpoints[0], goodpoints[len(goodpoints)-1])
                        goodtripduration = goodpoints[len(goodpoints)-1]['time'] - goodpoints[0]['time']
                        if goodtripduration.total_seconds() > 0:
                            goodtrip_avgspeed = goodtripdistance/goodtripduration.total_seconds()
                        else:
                            goodtrip_avgspeed = 0
                        print("@@ good part of recorded trip: d=", goodtripdistance, ", duration=",goodtripduration, ", goodtrip_avgspeed=", goodtrip_avgspeed)
                                                            
                        legdistance = leg['distance']
                        legduration = leg['duration']
                        if legduration > 0:                        
                            legavgspeed = round(legdistance/legduration)                    
                        else:
                            legavgspeed = 0
                        legheadway = None
                        if 'headway' in leg: 
                            legheadway = leg['headway']                    
                        print("@@ planned leg: d=", legdistance, ", duration=",timedelta(seconds=legduration), ", avg-speed=", legavgspeed, "headway:", legheadway)
                        # TODO check distances , check average speeds, ... 
                    
                                       
                    # COND1 -------------------:         
                    class PointMatchPair:
                        point1 = None
                        point2 = None                                          

                    MIN_MATCH_RATIO = 70 # (if 30%+ of recorded goodpoints don't have a match in plannedpoints, then routes do NOT match)
                    MAX_SERIAL_UNMATCH = 4 # TODO, how to calculate this number??? ex: consider vehicle speed? if faster, then limit is 2 !?                   
                                           # when goodpoints are chosen with min 100 m distance => 4 serials = min 400 meters
                    matchedpointpairs = []
                    unmatchedpoints = []
                    serialunmatches = [0]
                    
                    if hasgoodpoints:                                  
                        serialunmatchcount = 0 # number of consecutive goodpoints with no match in planned points
                        print("Matching goodpoints n=",len(goodpoints)," with plannedpoints m=",len(plannedpoints), "...")
                        # TODO make more efficient, make less than O(n^2)                  
                        # traverse goodpoints and try to match each with a planpoint:      
                        for point in goodpoints:
                            point_location = json.loads(point["geojson"])["coordinates"]
                            point_location_str='{1},{0}'.format(point_location[0],point_location[1])
                            point_time  = point['time']                        
                            
                            # search for its match in planned points
                            matchfound = False
                            deltas = []
                            mindelta = MAX_DISTANCE_FOR_POINT_MATCH
                            matchpair = PointMatchPair()         
                                           
                            for planpoint in plannedpoints: # traverse plannedpoints to find a match
                                planpoint_reverse = planpoint[1],planpoint[0]                                                     
                                delta = get_distance_between_coordinates(json.loads(point["geojson"])["coordinates"], planpoint_reverse)
                                deltas.append(delta)
                                if delta <= MAX_DISTANCE_FOR_POINT_MATCH: #found a match
                                    matchfound = True
                                    if delta <= mindelta: 
                                        mindelta = delta                               
                                        matchpair.point1 = point
                                        matchpair.point2 = planpoint                                

                            print("min delta for this goodpoint:", min(deltas))
                            if matchfound:
                                matchedpointpairs.append(matchpair)
                                if serialunmatchcount > 0:
                                    serialunmatches.append(serialunmatchcount)
                                serialunmatchcount = 0 # reset
                            else:
                                unmatchedpoints.append(point)
                                serialunmatchcount += 1
                        #END for, traverse goodpoints -----------
                        print("serialunmatches:", serialunmatches)
                                            
                        matched_fraction = round( 100 * ( len(matchedpointpairs)/float(len(goodpoints)) ) )
                        longest_serialunmatch = max(serialunmatches)                 
                        if matched_fraction >= MIN_MATCH_RATIO and longest_serialunmatch <= MAX_SERIAL_UNMATCH:
                            matchedbyroute = True   # this itin has a match also geoloc&route-based *
                            ridematched_str = ":::::: this leg might be a match (time-based & route-based) !!"                                        
                        print("matched_fraction", matched_fraction, "%,  MIN_MATCH_RATIO:", MIN_MATCH_RATIO,"%")
                        print("longest_serialunmatch:",longest_serialunmatch, "points,  MAX_SERIAL_UNMATCH:", MAX_SERIAL_UNMATCH)


                    # TODO: just print for debug ---------------------------------------          
                    if LOG_DETAILS:          
                        print("@@@ printing recorded points (n=",len(recordedpoints),"): ")                         
                        for point in recordedpoints:
                            point_location = json.loads(point["geojson"])["coordinates"]
                            point_location_str='{1},{0}'.format(point_location[0],point_location[1])                       
                            print(point_location_str)
                        print("@@@ printing goodpoints (n=",len(goodpoints),"): ")                         
                        for point in goodpoints:
                            point_location = json.loads(point["geojson"])["coordinates"]
                            point_location_str='{1},{0}'.format(point_location[0],point_location[1])
                            print(point_location_str)
                        
                        print("@@@ printing planned trip (n=",len(plannedpoints),"):")
                        for point in plannedpoints:
                            point_location = point
                            point_location_str='{0},{1}'.format(point_location[0],point_location[1])
                            print(point_location_str)                                        
                        intermstops = leg['intermediateStops']
                        print("@@@ printing intermediate stops of planned trip (n=",len(intermstops),"):")
                        for stop in intermstops: 
                            stopname = stop['name'].encode('utf-8')
                            print("stop[{3}][{4}]: {0}, @{1} .. @{2}".format(stopname, \
                                    HSLTimeStampToNormalDateTime(stop['arrival']), HSLTimeStampToNormalDateTime(stop['departure']), \
                                    stop['stopIndex'], stop['stopSequence']))

                        print("@@@ matched goodpoints (n=",len(matchedpointpairs),"):")
                        for pointpair in matchedpointpairs:
                            point = pointpair.point1
                            point_location = json.loads(point["geojson"])["coordinates"]
                            point_location_str1='{1},{0}'.format(point_location[0],point_location[1])
                            point = pointpair.point2
                            point_location = point
                            point_location_str2='{0},{1}'.format(point_location[0],point_location[1])
                            point_location_str='{0} --> {1}'.format(point_location_str1, point_location_str2)
                            print(point_location_str)                                        
                        print("@@@ unmatched goodpoints (n=",len(unmatchedpoints),"):")
                        for point in unmatchedpoints:
                            point_location = json.loads(point["geojson"])["coordinates"]
                            point_location_str='{1},{0}'.format(point_location[0],point_location[1])
                            print(point_location_str)                                        
                    
                    
                if matchedbytime: #and matchedbyroute: # save this leg as a match * # TODO!!! check both!
                    matchcount += 1 # number of total matches found so far (among all planned itins)

                    plannedmatch = PlannedTrip() # a new matched planned trip
                    
                    #these value comes from the itin itself, not from this leg::                    
                    plannedmatch.start = starttime
                    plannedmatch.end = endtime
                    plannedmatch.deltaT = deltaT
                    plannedmatch.deltaTsign = deltaTsign
                    #these value comes from this matched leg::
                    plannedmatch.linetype = mode
                    plannedmatch.linename = line
                    plannedmatch.legstart = legstarttime
                    plannedmatch.legend = legendtime
                    plannedmatch.deltaStarttime = deltaStarttime
                    if legstarttime < trip_starttime:
                        plannedmatch.deltaStarttimeStr = ("-{0}").format(deltaStarttime) # planned transitleg starts earlier than recorded starttime
                    else:
                        plannedmatch.deltaStarttimeStr = ("+{0}").format(deltaStarttime) # planned transitleg starts later or same time                                   
                    plannedmatch.deltaStartPassed = deltaStartPassed
                    plannedmatch.matchedbytime = matchedbytime
                    plannedmatch.matchedbyroute = matchedbyroute     
                    plannedmatch.matched_fraction = matched_fraction       
                    plannedmatch.longest_serialunmatch = longest_serialunmatch        
                    # TODO: also save ??? will be useful??:
                    #   legloc start, end
                    #   leg geo points                                                                                                                   

                    plannedmatches.append(plannedmatch)
                #END IF ---   
                
                legindex += 1    
            #LOOP END -- traverse next leg of current itin            
        #else END
        
        # --- alll legs of current itin has been traveresed up to this point 
        # assumption for now: only 1 trip-leg (transit leg) from each planned itin can be a match
        # TODO: support multi-leg matches later asap *!!
        if legsmatched > 0: # we've found transit-leg(s) as a match in this itin (ideally should be only 1 matched leg (the only transit leg))
            #planned_start.append(starttime)
            #planned_end.append(endtime)
            donothing = None # TODO
        
        if not legsreviewed: # TODO ***
            print("Legs > > > > > > : ")
            for leg in itin['legs']:
                mode = leg['mode']
                line = leg['route']
                istransit = leg['transitLeg']
                legduration = leg['duration']
                legdurationnormal = HSLDurationToNormalDuration(int(legduration)) # TODO why int() ?
                legstarttime = HSLTimeStampToNormalDateTime(leg['startTime'])
                legendtime = HSLTimeStampToNormalDateTime(leg['endTime'])

                #line name encoding, now considered utf-8 #TODO
                line_name_str = "None"
                if line: 
                    line_name_str = line.encode('utf-8')                
                print(mode, line_name_str, ", is transit:", istransit, "| Duration: ", \
                        legdurationnormal, "(",legstarttime,"-->",legendtime,")")
        
        itin_index += 1
    # LOOP END --- traverse next planned itin
    
    print("")
    print("from all planned itins ==>")
    print("matchcount:", matchcount)    
    bestmatchindex = 0
    # refining the matched legs from all planned itins, choosing the best match: 
    # assumption: HSL journey planner returns the best match first!
    if len(plannedmatches)>0:
        print("Refining the matched planned trips ...")
        min_deltaStarttime = plannedmatches[0].deltaStarttime
        matchindex = 0
        for match in plannedmatches:
            print("[",matchindex,"]: match.deltaStarttime:", match.deltaStarttime)    
            if match.deltaStarttime < min_deltaStarttime:
                min_deltaStarttime = plannedmatches[matchindex].deltaStarttime
                bestmatchindex = matchindex
            matchindex += 1    
        print("bestmatchindex:", bestmatchindex)

    if len(plannedmatches)>0:
        tripmatchres.trip = plannedmatches[bestmatchindex]        
        tripmatchres.bestmatchindex = bestmatchindex        
        tripmatchres.matchcount = matchcount                
        return 1, tripmatchres
    else:
        return 1, tripmatchres

# ---------------------------------------------------------


#def main_function(): # note: function to be called if want to run this file independently (e.g: only test querying HSL journey planner)
    # ------ inputs ----------------------------------

    # Examples, trips, legs, etc.
    # "otakaari 1": 60.186310, 24.828335
    # "otaniementie 7": 60.183924,24.828918
    # "otaniementie 9": 60.184430, 24.828404

    # from recorded trips: 
    # 7 | POINT(24.907521 60.190996) | 249076601908 | 2016-01-25 12:20:35.828 | IN_VEHICLE | BUS       | 18		(Haartmaninkatu 6 --> Otaniementie 8)
    # 7 | POINT(24.829176 60.184118) | 248299601838 | 2016-01-25 12:33:25.754 | IN_VEHICLE | BUS       | 18

    # 7 | POINT(60.170718, 24.930221) | 249317601711 | 2016-02-05 17:20:03.78  | IN_VEHICLE | BUS       | 70T
    # 7 | POINT(60.250214, 25.009566) | 250167602546 | 2016-02-05 17:54:01.755 | IN_VEHICLE | BUS       | 70T

    # 14 | POINT(60.209801,25.037541) | 250378602095 | 2016-01-10 15:16:50.319 | IN_VEHICLE | BUS       | 79
    # 14 | POINT(60.225745,25.024415) | 250238602256 | 2016-01-10 15:23:21.386 | IN_VEHICLE | BUS       | 79

    # ??
    # 14 | POINT(60.206303,25.044673) | 250440602066 | 2016-01-10 21:20:44.347 | IN_VEHICLE | BUS       | 79


    #fromPlace="60.209801,25.037541"
    #toPlace="60.225745,25.024415"
    #i_starttime = datetime.strptime('2016/05/01 15:16:50', '%Y/%m/%d %H:%M:%S')
    #i_endtime = datetime.strptime('2016/05/01 15:23:21', '%Y/%m/%d %H:%M:%S')


