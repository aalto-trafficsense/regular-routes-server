
from sqlalchemy.sql import text
import json
import gmplot

from pyfiles.common_helpers import (pointRow_to_geoText, geoLoc_to_pointRow, geoJSON_to_geoText, 
                                    pointRow_to_postgisPoint, DateTime_to_Text)
from pyfiles.logger import(log, loge, logi)


class Visualize:
    def __init__(self,  db):
        self.db = db

    #def map_trip_points(self, trips):

    #def save_trip_points_to_file(self, trips):
                

    def get_trip_points(self, user_ids,  trip_ids,  plan_id):
        #plannedpoints = polyline.decode(leg['legGeometry']['points'])
        qstr = """
            SELECT user_id, id, plan_id, start_time, end_time, duration, mainmode, multimodal_summary 
            FROM trips 
            WHERE user_id IN ({0}) AND id IN ({1}) AND plan_id = {2} 
            ORDER BY user_id, start_time, id, plan_id;
        """.format(user_ids,  trip_ids,  plan_id) 
        log(["get_trip_points():: qstr:", qstr])
        log("")                
        trips =  self.db.engine.execute(text(qstr))

        if trips.rowcount>0:
            for trip in trips: # show points of each trip
                user_id = trip['user_id']
                trip_id = trip['id']
                
                qstr = """
                    SELECT time, ST_AsGeoJSON(coordinate) as coo, activity, line_type, line_name 
                    FROM device_data_filtered 
                    WHERE user_id = {0} 
                    AND time>='{1}' AND time<='{2}'
                    ORDER BY time;
                """.format(user_id,  DateTime_to_Text(trip['start_time']),  DateTime_to_Text(trip['end_time']))
                log(["get_trip_points():: qstr:", qstr])
                log("")                                
                trip_points = self.db.engine.execute(text(qstr))
                
                if trip_points.rowcount > 0:                    
                    #print user_id, trip_id, plan_id
                    
                    # extarct all trip points (geolocs)
                    class LatsLons:
                        def __init__(self):
                            self.latitudes = []
                            self.longitudes = []
                            
                        def append(self,  lat, lon):
                            self.latitudes.append(lat)
                            self.longitudes.append(lon)
                                                
                    latitudes = []
                    longitudes = []
                                        
                    redpoints = LatsLons()
                    bluepoints = LatsLons()
                    greenpoints = LatsLons()
                    graypoints = LatsLons()
                    
                    odpoints = LatsLons()
                    opoints = LatsLons()
                    dpoints = LatsLons()
                    
                    for trip_point in trip_points:
                        #print geoJSON_to_geoText(trip_point['coo'])
                        lat = json.loads(trip_point['coo'])["coordinates"][1]
                        lon = json.loads(trip_point['coo'])["coordinates"][0]
                        latitudes.append(lat)
                        longitudes.append(lon)
                        
                        if trip_point['activity'] == 'IN_VEHICLE':
                            if trip_point['line_type'] is None:
                                redpoints.append(lat, lon)
                            else:
                                # TODO: old code: colors.append('blue') # public transport (PT)
                                bluepoints.append(lat, lon) # public transport (PT)
                        elif trip_point['activity'] in {'RUNNING', 'WALKING',  'ON_BICYCLE'}:
                            greenpoints.append(lat, lon)
                        else:
                            graypoints.append(lat, lon)
                
                    
                    odpoints.append(latitudes[0], longitudes[0])
                    opoints.append(latitudes[0], longitudes[0])
                    odpoints.append(latitudes[len(latitudes)-1], longitudes[len(longitudes)-1])
                    dpoints.append(latitudes[len(latitudes)-1], longitudes[len(longitudes)-1])
                    
                    # draw on map
                    #gmap = gmplot.from_geocode("Helsinki")
                    gmap = gmplot.GoogleMapPlotter(60.18, 24.78, 12)
                    
                    gmap.scatter(opoints.latitudes, odpoints.longitudes, 'w', marker=True) # origin
                    gmap.scatter(dpoints.latitudes, dpoints.longitudes, 'k', marker=True) # destination
                    
                    gmap.scatter(redpoints.latitudes, redpoints.longitudes, '#BB0000', size=20, marker=False)
                    gmap.scatter(bluepoints.latitudes, bluepoints.longitudes, '#0000BB', size=20, marker=False)
                    gmap.scatter(greenpoints.latitudes, greenpoints.longitudes, '#00BB00', size=20, marker=False)
                    gmap.scatter(graypoints.latitudes, graypoints.longitudes, '#AAAAAA', size=20, marker=False)
                    
                    gmap.plot(latitudes, longitudes, '#BBBBBB', edge_width=3)
                                                            
#                    gmap.plot(redpoints.latitudes, redpoints.longitudes, '#BB0000', edge_width=5)
#                    gmap.plot(bluepoints.latitudes, bluepoints.longitudes, '#0000BB', edge_width=5)
#                    gmap.plot(greenpoints.latitudes, greenpoints.longitudes, '#00BB00', edge_width=5)
#                    gmap.plot(graypoints.latitudes, graypoints.longitudes, '#AAAAAA', edge_width=5)
                    
                    htmlfilename = 'trip_points_' + str(user_id) + '_' + str(trip_id) + '_' + str(plan_id) + '.html'
                    #  datetime.datetime.now().time()
                    htmlfilename = "/home/mehrdad/" + htmlfilename                    
                    gmap.draw(htmlfilename)

#                    gmap = gmplot.GoogleMapPlotter(60.15, 24.70, 12)                    
#                    gmap.plot(latitudes, longitudes, '#AAAAAA', edge_width=5)
#                    htmlfilename = htmlfilename + ".html"
#                    gmap.draw(htmlfilename)

                #SELECT time, ST_AsGeoJSON(coordinate), activity, line_type, line_name FROM device_data_filtered WHERE time>='2016-11-18 18:27:57' AND time<='2016-11-18 18:57:07';

