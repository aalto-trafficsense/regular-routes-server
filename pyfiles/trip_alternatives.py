
from pyfiles.common_helpers import (pointRow_to_geoText, round_dict_values, DateTime_to_Text, DateTimeDelta_to_Text)
from pyfiles.trip_economics import (TripEconomics)
from pyfiles.trip_planner import (TripPlanner, PublicTransportModesTemplate)
from pyfiles.logger import(log, loge, logi)
from copy import deepcopy

# -----------------------------------------

UserModalChoiceTemplate = {'CAR':0, 'PUBLIC_TRANSPORT':0, 'BICYCLE':0, 'WALK':0}

PlanIDModeTemplate = {"plan_id":None, "mode": None}
BestModalChoiceByParamTemplate = {'time':deepcopy(PlanIDModeTemplate), 'cost':deepcopy(PlanIDModeTemplate), \
                                  'emission':deepcopy(PlanIDModeTemplate), 'cals':deepcopy(PlanIDModeTemplate), 'comfort':deepcopy(PlanIDModeTemplate)}

CarPublicTemplate = {'CAR':0, 'PUBLIC_TRANSPORT':0}
CLCarPublicTemplate = {'time':deepcopy(CarPublicTemplate), 'cost':deepcopy(CarPublicTemplate), \
                       'emission':deepcopy(CarPublicTemplate), 'cals':deepcopy(CarPublicTemplate), 'comfort':deepcopy(CarPublicTemplate)}                       

CarPublicBikeTemplate = {'CAR':0, 'PUBLIC_TRANSPORT':0, 'BICYCLE':0}
CLCarPublicBikeTemplate = {'time':deepcopy(CarPublicBikeTemplate), 'cost':deepcopy(CarPublicBikeTemplate), \
                           'emission':deepcopy(CarPublicBikeTemplate), 'cals':deepcopy(CarPublicBikeTemplate), 'comfort':deepcopy(CarPublicBikeTemplate)}

CarPublicWalkTemplate = {'CAR':0, 'PUBLIC_TRANSPORT':0, 'WALK':0}
CLCarPublicWalkTemplate = {'time':deepcopy(CarPublicWalkTemplate), 'cost':deepcopy(CarPublicWalkTemplate), \
                           'emission':deepcopy(CarPublicWalkTemplate), 'cals':deepcopy(CarPublicWalkTemplate), 'comfort':deepcopy(CarPublicWalkTemplate)}

PublicBikeTemplate = {'BICYCLE':0, 'PUBLIC_TRANSPORT':0}
CLPublicBikeTemplate = {'time':deepcopy(PublicBikeTemplate), 'cost':deepcopy(PublicBikeTemplate), \
                       'emission':deepcopy(PublicBikeTemplate), 'cals':deepcopy(PublicBikeTemplate), 'comfort':deepcopy(PublicBikeTemplate)}                       

CarPublicBikeWalkTemplate = {'CAR':0, 'PUBLIC_TRANSPORT':0, 'BICYCLE':0, 'WALK':0} #TODO , can influene priority while comparing, if use list [] and order it ??
CLCarPublicBikeWalkTemplate = {'time':deepcopy(CarPublicBikeWalkTemplate), 'cost':deepcopy(CarPublicBikeWalkTemplate), \
                           'emission':deepcopy(CarPublicBikeWalkTemplate), 'cals':deepcopy(CarPublicBikeWalkTemplate), 'comfort':deepcopy(CarPublicBikeWalkTemplate)}

# -----------------------------------------
class TripAlternatives:
    def __init__(self,  db):
        self.comparison_list_car_public = {} # example: {13:{'time':{'CAR':3, 'transit':7}, 'cost':{'CAR':6, 'transit':4}}, 6:{'cost':{'CAR':6, 'transit':4}}}
        self.comparison_list_car_public_bike = {}
        self.comparison_list_car_public_walk = {}
        self.comparison_list_public_bike = {}        
        self.comparison_list_car_public_bike_walk = {}
        self.user_modalchoice_list = {}
        self.db = db
    
    # for each trip plans its alternatives and adds them to trip.alternative_trips
    def plan_trip_alternatives(self, trips):    
        planner = TripPlanner(self.db)
        for trip in trips:
            desiredModes = ['WALK', 'BICYCLE', 'CAR', 'PUBLIC_TRANSPORT'] # TODO enable, disable next line (maybe not, list is ordered so it's better)
            # desiredModes = {'WALK', 'BICYCLE', 'CAR'}
            for mode in desiredModes:            
                res, planned_trips, planning_res = planner.plan_a_trip(trip, mode)            
                if res == 1:
                    for planned_trip in planned_trips:
                        planned_trip.plan_id = len(trip.alternative_trips) + 1
                        trip.alternative_trips.append(planned_trip)
                elif res == 0:
                    loge(["trip:", trip.user_id, trip.id, ": mode", mode, ":: planning error:", planning_res.to_Text()])
                
    def show_trips_and_alternatives(self,  trips):
        # show calculated params:
#        for trip in trips:
#            self.display_trip_economics(trip)
#            for tripalt in trip.alternative_trips:
#                self.display_trip_economics(tripalt)
#            print ""
        self.display_trip_economics_header_csv()
        self.display_trip_economics_units_csv()
        
        return 1 #TODO: temporary disabled following detailed logs: 
        for trip in trips:
            self.display_trip_economics_csv(trip)
            for tripalt in trip.alternative_trips:
                self.display_trip_economics_csv(tripalt)
            print ""
        
        
    def compare_trip_alternatives(self, trips):                
        for trip in trips:
            # compare params of the trip and its alt trips in order to set pros & cons of modal-choices per trip 
            # (eg. alt trip #2 with WALK->CAR is the fastest choice => 'WALK->CAR' is faster modal-choice for this trip):
            best_modalchoice_by_param_car_public = self.get_best_modalchoice_by_param_for_trip(trip, CarPublicTemplate)
            best_modalchoice_by_param_car_public_bike = self.get_best_modalchoice_by_param_for_trip(trip, CarPublicBikeTemplate)
            best_modalchoice_by_param_car_public_walk = self.get_best_modalchoice_by_param_for_trip(trip, CarPublicWalkTemplate)                        
            best_modalchoice_by_param_public_bike = self.get_best_modalchoice_by_param_for_trip(trip, PublicBikeTemplate)                                   
            best_modalchoice_by_param_car_public_bike_walk = self.get_best_modalchoice_by_param_for_trip(trip, CarPublicBikeWalkTemplate)                                   

            # summarize (cumulative) pros and cons for per user *:
            # (eg. of total 10 trips of user 1: 'WALK->CAR' is the fastest modal-choice in 8 trips and 'BICYCLE' in 2 trips)
            self.increase_pros_for_user(trip.user_id, self.comparison_list_car_public, CLCarPublicTemplate, best_modalchoice_by_param_car_public)
            self.increase_pros_for_user(trip.user_id, self.comparison_list_car_public_bike, CLCarPublicBikeTemplate, best_modalchoice_by_param_car_public_bike)
            self.increase_pros_for_user(trip.user_id, self.comparison_list_car_public_walk, CLCarPublicWalkTemplate, best_modalchoice_by_param_car_public_walk)
            self.increase_pros_for_user(trip.user_id, self.comparison_list_public_bike, CLPublicBikeTemplate, best_modalchoice_by_param_public_bike)
            self.increase_pros_for_user(trip.user_id, self.comparison_list_car_public_bike_walk, CLCarPublicBikeWalkTemplate, best_modalchoice_by_param_car_public_bike_walk)
                        
            # get what user has chosen (from the actual trip) -----------------
            # pass this: self.user_modalchoice_list                         
            self.increase_modalchoice_for_user(trip.user_id, trip, self.user_modalchoice_list)            
            
        return 1

    def increase_modalchoice_for_user(self, user_id, trip, user_modalchoice_list):
        log(["increase_modalchoice_for_user:: ..."])
        log(["trip.mainmode:",trip.mainmode])
        chosenmode = trip.mainmode
        if chosenmode in PublicTransportModesTemplate:
            chosenmode = 'PUBLIC_TRANSPORT' 
        log(["chosenmode:",chosenmode])

        if user_id not in user_modalchoice_list:
            user_modalchoice_list[user_id] = deepcopy(UserModalChoiceTemplate)            

        if chosenmode in user_modalchoice_list[user_id]:
            user_modalchoice_list[user_id][chosenmode] += 1    
    
    def get_best_modalchoice_by_param_for_trip(self, trip, desired_modes_template):
        # find the best modal-choice per trip depending on the target param (eg. 'time', 'cals', ...):
        mintime = None # TODO: or compare with original trip?!
        mincost = None
        maxcals = None
        minemission = None
        maxcomfort = None
        pros = deepcopy(BestModalChoiceByParamTemplate)
        # pros_detailed = deepcopy(BestModalChoiceByParamTemplate) # TODO later
        modes_to_compare = []
        for mode, val in desired_modes_template.iteritems():
#            if mode == 'PUBLIC_TRANSPORT':
#                for public_transport_mode in PublicTransportModesTemplate:
#                    modes_to_compare.append(mode)                    
            modes_to_compare.append(mode)
        
        log([""])
        log(["get_best_modalchoice_by_param_for_trip:: ...:"])
        log(["modes_to_compare:", modes_to_compare])
                
        #if trip.user_id not in comparison_list:
        #    comparison_list[trip.user_id] = desiredCLTemplate #TODO old code?! remove?
        
        # find the min val or max val depending on the target param 
        # (eg. for 'cals' we should find the tripalt (modal-choice chain) with largest 'cals' val)
        for tripalt in trip.alternative_trips:
            log(["tripalt.mainmode:",tripalt.mainmode])
            mainmode = tripalt.mainmode
            if mainmode in PublicTransportModesTemplate:
                mainmode = 'PUBLIC_TRANSPORT' 
            log(["mainmode:",mainmode])
            
            if mainmode in modes_to_compare:
                if mintime is None or tripalt.duration < mintime: #TODO : what if == ???
                    mintime = tripalt.duration                    
                    pros['time'] = {"plan_id":tripalt.plan_id, "mode": mainmode}
                if mincost is None or tripalt.cost < mincost:
                    mincost = tripalt.cost
                    pros['cost'] = {"plan_id":tripalt.plan_id, "mode": mainmode}
                if maxcals is None or tripalt.calories > maxcals:
                    maxcals = tripalt.calories
                    pros['cals'] = {"plan_id":tripalt.plan_id, "mode": mainmode}
                if minemission is None or tripalt.emission < minemission:
                    minemission = tripalt.emission
                    pros['emission'] = {"plan_id":tripalt.plan_id, "mode": mainmode}
                if maxcomfort is None or tripalt.comfort > maxcomfort:
                    maxcomfort = tripalt.comfort                    
                    pros['comfort'] = {"plan_id":tripalt.plan_id, "mode": mainmode}

        log([trip.user_id,trip.device_id, trip.id,":"," pros:",pros])
        return pros            

    def increase_pros_for_user(self, user_id, comparison_list, cl_desired_template, best_modalchoice_by_param): 
        log([""])
        log(["increase_pros_for_user ...:"])
        log(["cl_desired_template:",cl_desired_template])
        log(["best_modalchoice_by_param:",best_modalchoice_by_param])
        if user_id not in comparison_list:
            comparison_list[user_id] = deepcopy(cl_desired_template)
        log(["comparison_list before calcs:",comparison_list])
        
        for param, choice in best_modalchoice_by_param.iteritems():
            if choice['mode'] is not None:
                comparison_list[user_id][param][choice['mode']] += 1

        log(["comparison_list after calcs:",comparison_list])

    #--------------------------------------------------------------------------------------------------    
    def display_trip_economics(self, trip):
        print trip.user_id,"|",trip.id, ",", trip.plan_id, \
              "|", trip.multimodal_summary, \
              "|", DateTime_to_Text(trip.starttime), "to", DateTime_to_Text(trip.endtime), \
              "| from ", pointRow_to_geoText(trip.origin), "--> to", pointRow_to_geoText(trip.destination), \
              "| time:", DateTimeDelta_to_Text(trip.duration), "| cost:", round(trip.cost, 2), round_dict_values(trip.cost_by_mode, 2), \
              "| cals:", int(round(trip.calories)), round_dict_values(trip.calories_by_mode, 0), \
              "| emission:", int(round(trip.emission)), round_dict_values(trip.emission_by_mode, 0), \
              "| comfort:", trip.comfort, "| distance: ", int(round(trip.distance)), round_dict_values(trip.distance_by_mode, 0)

    def display_trip_economics_header_csv(self):
        print "user-id; device_id; trip-id; trip-plan-id; multimodal summary; start-time;end-time;trip-time;cost;calories;emission;comfort;distance;;from;to;;"\
               "cost by mode;calories by mode;emission by mode;distance by mode"

    def display_trip_economics_units_csv(self):
        print ";;;;;;;;(eur);(cals);(co2 kg);(%);(km);;;;;(eur);(cals);(co2 grams);(m)"
        
    def display_trip_economics_csv(self, trip):                 
        print trip.user_id,"|", trip.device_id,"|", trip.id, "|", trip.plan_id, \
              "|", trip.multimodal_summary, \
              "|", DateTime_to_Text(trip.starttime), "|", DateTime_to_Text(trip.endtime), \
              "|", DateTimeDelta_to_Text(trip.duration), "|", round(trip.cost, 2), \
              "|", int(round(trip.calories)), "|", round(trip.emission/1000.0, 1), \
              "|", trip.comfort, "|", round(trip.distance/1000.0, 1), \
              "||", pointRow_to_geoText(trip.origin), "|", pointRow_to_geoText(trip.destination),\
              "||", round_dict_values(trip.cost_by_mode, 2), "|", round_dict_values(trip.calories_by_mode, 0), \
              "|", round_dict_values(trip.emission_by_mode, 0),"|", round_dict_values(trip.distance_by_mode, 0)

    def display_trip_alternatives_comparison_summary(self):
        # show summary of pros and cons per user:
        logi(["Showing summary of pros and cons per user: ...:"])
        logi([""])
        logi(["Car vs. Public Transport:"])
        for user, comparison_list in self.comparison_list_car_public.iteritems():
            logi(["user", user, comparison_list])

        logi([""])
        logi(["Car vs. Public Transport vs. Bike:"])
        for user, comparison_list in self.comparison_list_car_public_bike.iteritems():
            logi(["user", user, comparison_list])

        logi([""])
        logi(["Car vs. Public Transport vs. Walk:"])
        for user, comparison_list in self.comparison_list_car_public_walk.iteritems():
            logi(["user", user, comparison_list])

        logi([""])
        logi(["Public Transport vs. Bike:"])
        for user, comparison_list in self.comparison_list_public_bike.iteritems():
            logi(["user", user, comparison_list])

        logi([""])
        logi(["Car vs. Public Transport vs. Bike vs. Walk:"])
        for user, comparison_list in self.comparison_list_car_public_bike_walk.iteritems():
            logi(["user", user, comparison_list])

        logi([""])
        logi(["You chose:"])
        for user, modalchoices in self.user_modalchoice_list.iteritems():
            logi(["user", user, modalchoices])
        
