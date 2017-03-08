
from datetime import timedelta
from pyfiles.trip_planner import (PublicTransportModesTemplate)
from pyfiles.logger import (logi, loge, log)

#NOTE: OTP modes ref: http://dev.opentripplanner.org/apidoc/0.15.0/ns0_TraverseMode.html
#TODO update code: TRAM, RAIL, SUBWAY emission should NOT be 0, as it sould include emission from electricity production
# following values are *after* considering avg passengers per transit vehicle (except* 'CAR' for which passenger/car is retrieved from user profile)
emissionsPerPassengerKmByMode = {"WALK":0, "RUN":0, "BUS": 73, "TRAM":0, "RAIL":0, "FERRY":389, "CAR":171, "SUBWAY":0, "BICYCLE":0, "EBICYCLE":0} # g/passenger-km

caloriesPerKmByMode = {"WALK":72.3, "RUN":72.3, "BICYCLE":16.2, "EBICYCLE":13} #NOTE: refer to sushi docs (walk cals = run cals)

oneTimeCostByMode = {"BUS":3 , "TRAM":1.5, "RAIL":3, "FERRY":3, "SUBWAY":3}  # eur
monthlyCostByMode = {"BUS":102 , "TRAM":102, "RAIL":102, "FERRY":102, "SUBWAY":102} # eur #TODO assumption: there's one monthly pass covering all modes

intermediateMode = 'OTHER'

# TODO values for now based on Volkswagen Golf 1.4 90 KW Trendline (Or Equivalent New Car)
# TODO get from user profile later
fuelPer100KmByVehicleType = {"hatchback": 9.3} # unit: litre/100km
fuelCostPerLiterByCity = {"helsinki": 1.6} # eur
depreciationPerKmByVehicleType = {"hatchback": 0.0625} # eur
carInsurance = 200 # eur

# TODO: get from user profile later
has_montly_pass = True
passengers_per_car = 1
avg_trips_per_day = 2


# -------- trip economics, comparison, SUSHI, etc. --------------------------    
class TripEconomics:
    def __init__(self):
        self.config1 = None

    def get_trips_economics(self, trips):
        # calculate params for each trip and alternative trips assigned to it
        for trip in trips:
            # calculate params per trip (each trip has a modal-choice chain eg. WALK->CAR that results in certain param values e.g.: time:x, cost:y, ...):
            self.calculate_trip_economics(trip)
            for tripalt in trip.alternative_trips:
                self.calculate_trip_economics(tripalt)                
                
        
    def calculate_trip_economics(self, trip):
        # note: order is partly important
        #try:
            self.get_trip_time(trip)
            self.get_trip_distances(trip)
            self.get_trip_cost(trip)
            self.get_trip_calories(trip)
            self.get_trip_emission(trip)
            self.get_trip_comfort(trip)                
            self.get_trip_mainmode(trip)
            self.get_trip_multimodal_summary(trip)
        #except Exception as e:
        #    loge(["trip:",trip.user_id,"|",trip.device_id,"|",trip.id,"|",trip.plan_id, " - (!) EXCEPTION catched in calculate_trip_economics():", e])
    
    def get_trip_mainmode(self, trip):
        trip.mainmode = None
        if len(trip.legs) == 1:
            leg = trip.legs[0]
            trip.mainmode = leg['mode']
        else:
            for leg in trip.legs: # TODO need to change later? #TODO: how to have a 'main mode' for multimodal trips? eg.: walk -> bike -> train -> bike -> walk
                if leg['mode'] in {'CAR', 'BICYCLE'} or leg['mode'] in PublicTransportModesTemplate:
                    trip.mainmode = leg['mode']
                    break
                    
        startted_with_walk = (trip.legs[0]=='WALK')
        ended_with_walk = (trip.legs[-1]=='WALK')
        
        if trip.mainmode is None: # TODO: improve later. possible exceptions when this wouldn't recognize 'WALK'?
            trip.mainmode = trip.legs[0]['mode']

    def get_trip_multimodal_summary(self, trip):
        modalstr = ""
        for leg in trip.legs:
            if modalstr != "":
                modalstr += "->"
            modalstr += leg['mode']
        trip.multimodal_summary = modalstr

   
    def get_trip_time(self, trip):
        trip.duration = trip.endtime - trip.starttime
        trip.duration_by_mode = {}
        total_legs_duration = timedelta(0)
        for leg in trip.legs:
            legduration = leg['duration']
            trip.add_duration(leg['mode'], legduration) # for now 'total' trip duration is NOT calculated in this function
            total_legs_duration += legduration
        trip.add_duration(intermediateMode, trip.duration - total_legs_duration)

    def get_trip_distances(self, trip):
        trip.distance = 0
        trip.distance_by_mode = {}
        for leg in trip.legs:            
            # distance by mode:
            trip.add_travelled_distance(leg['mode'], leg['distance']) # NOTE: 'total distance' of trip is increased inside this function
        
    def get_trip_cost(self, trip):
        trip.cost = 0
        trip.cost_by_mode = {}
        paid_for_transit = False
        for leg in trip.legs:
            # TODO: assumption, if user pays for one ticket for 'tram', can get on 'bus' with the same ticket (TODO: later consider: ticket expires in ?? minutes)
            legcost = self.get_leg_cost(leg)
            if leg['transitLeg'] == True and paid_for_transit:
                legcost = 0                
            if leg['transitLeg'] == True:
                paid_for_transit = True
            trip.add_cost(leg['mode'], legcost) # NOTE: 'total cost' of trip is increased inside this function

    def get_leg_cost(self, leg):                        
        if leg['mode'] == 'CAR':            
            cost = ((leg['distance']/1000.0)/100.0) * fuelPer100KmByVehicleType['hatchback'] * fuelCostPerLiterByCity['helsinki'] + \
                   (leg['distance']/1000.0) * depreciationPerKmByVehicleType['hatchback'] + \
                   carInsurance/(365 * avg_trips_per_day)                               
            # note: refer to 'greener transportation ...' doc files
            #Trip cost = distance * gallon/km * petrol-price + 
            #            parking cost + 
            #            (yearly maintenance and costs)/(365 * trips-per-day)                        
            cost = cost/passengers_per_car
        elif leg['transitLeg'] == True:
            if has_montly_pass:
                cost_per_trip = float(monthlyCostByMode[leg['mode']])/(30.0 * avg_trips_per_day)
                cost = cost_per_trip
            else:
                cost = oneTimeCostByMode[leg['mode']] #TODO add condition for monthly ticket
        else:
            cost = 0
        return cost
               
    def get_trip_calories(self, trip):
        trip.calories = 0
        trip.calories_by_mode = {}
        trip.add_calories('WALK', self.get_trip_calories_by_mode(trip, 'WALK'))
        trip.add_calories('RUN', self.get_trip_calories_by_mode(trip, 'RUN'))
        trip.add_calories('BICYCLE', self.get_trip_calories_by_mode(trip, 'BICYCLE'))
        trip.add_calories('EBICYCLE', self.get_trip_calories_by_mode(trip, 'EBICYCLE'))
#        cals = 0
#        cals += self.get_trip_calories_by_mode(trip, 'WALK')
#        cals += self.get_trip_calories_by_mode(trip, 'RUN')
#        cals += self.get_trip_calories_by_mode(trip, 'BICYCLE')
#        cals += self.get_trip_calories_by_mode(trip, 'EBICYCLE')                                
#        trip.calories = cals        

    def get_trip_calories_by_mode(self, trip, mode):
        cals = (trip.get_distance_by_mode(mode)/1000.0) * caloriesPerKmByMode[mode]
        return cals
            
    def get_trip_emission(self, trip):    
        trip.emission = 0
        trip.emission_by_mode = {}
        for leg in trip.legs:
            #legemission = self.get_leg_emission(leg)
            #trip.emission += legemission
            trip.add_emission(leg['mode'], self.get_leg_emission(leg))
            
    def get_leg_emission(self, leg):                
        emission = (leg['distance']/1000.0) * emissionsPerPassengerKmByMode[leg['mode']]
        return emission

    def get_trip_comfort(self, trip):    
        trip.comfort = 50 # (%)
        # traverse all alerts of each leg
        try:
            for leg in trip.legs:            
                if 'alerts' in leg and leg['alerts'] is not None:
                    for alert in leg['alerts']:
                        if 'alertHeaderText' in alert and alert['alertHeaderText'] == 'Unpaved surface': #TODO is this even bad in case of 'WALK' or 'BICYCLE'?
                            trip.comfort -= 10
        except Exception as e:
            loge(["trip:",trip.user_id,"|",trip.device_id,"|",trip.id,"|",trip.plan_id, " - (!) EXCEPTION catched in trip_economics.get_trip_comfort():", e])

        
