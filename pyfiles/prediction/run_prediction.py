#!/usr/bin/python

'''
    runPrediction.py
    ----------------------------------
'''

import sys

# Scientific Libraries
from numpy import *
set_printoptions(precision=5, suppress=True)

# Provides snapping and stacking functionality
sys.path.append("./src")
from utils import *

from sklearn import ensemble
from ML import *

import joblib
##################################################################################
#
# Load trace
#
##################################################################################
import psycopg2

def predict(DEV_ID,win_past=5,win_futr=5,mod="EML",lim=0,commit_result=True):
    '''
        1. obtain full trace AFTER 'lim' (or else the most recent readings)
        consider that points are new and haven't been snapped yet, so
            2. fetch clusters from cluster_centers
            3. snap full trace to clusters
        4. load a model from disk
        5. stack and TEST a model
    '''

    try:
        conn = psycopg2.connect("dbname='regularroutes' user='regularroutes' host='localhost' password='TdwqStUh5ptMLeNl' port=5432")
    except:
        print "I am unable to connect to the database"

    c = conn.cursor()

    ##################################################################################
    #
    # Extract trace (most recent points)
    #
    ##################################################################################

    if lim <= 0:
        print "Extracting trace (previous 100 points)"
        c.execute('SELECT device_id,hour,day_of_week,longitude,latitude FROM averaged_location WHERE device_id = %s ORDER BY time_stamp DESC LIMIT 100', (str(DEV_ID),))
    else:
        print "Extracting trace (points from timestamp %d onwards)" % (lim)
        c.execute('SELECT device_id,hour,day_of_week,longitude,latitude FROM averaged_location WHERE device_id = %s AND time_stamp < %s ORDER BY time_stamp DESC', (str(DEV_ID),str(lim),))
    dat = array(c.fetchall(),dtype={'names':['d_id', 'H', 'DoW', 'lon', 'lat'], 'formats':['i4', 'i4', 'i4', 'f4','f4']})
    run = column_stack([dat['lon'],dat['lat']])
    X = column_stack([dat['lon'],dat['lat'],dat['H'],dat['DoW']])
    print "Extracted trace of ", len(run), "points."

    ##################################################################################
    #
    # Extract nodes
    #
    ##################################################################################

    print "Extracting waypoints"
    c.execute('SELECT latitude, longitude FROM cluster_centers WHERE device_id = %s', (str(DEV_ID),))
    rows = c.fetchall()
    nodes = array(rows)

    ##################################################################################
    #
    # Snapping
    #
    ##################################################################################
    print "Snapping past to these ", len(nodes)," waypoints"
    y = snap(run,nodes)

    print y

    print "Stack the ", y.shape, "points into an ML dataset ..."
    X,Y = stack_stream(X,y,win_past,win_futr)

    #print X.shape
    #print Y.shape

    print "Load model from disk ..."
    fname = "./dat/model_dev"+str(DEV_ID)+".model"
    h = joblib.load( fname)
    print "return ",  y.shape, "points into an ML dataset ..."

    print X.shape
    # IF THIS IS A SINGLE ENTRY, WE WILL NEED TO RECUT IT WITH x = X[-1,:] AND THEN DO predict(x)[0]
    #print "x=", x.shape

    print "Prediction: " ,

    yp = h.predict(X)

    if commit_result:

        print "Committing to table ... " ,
        for t in range(len(yp)):
            sql = "INSERT INTO predictions (device_id, cluster_id, time_stamp, time_index) VALUES (%s, %s, NOW(), %s)"
            c.execute(sql, (str(DEV_ID), str(yp[t]), str(t+1)))

        conn.commit()

        c.execute('SELECT cluster_centers.cluster_id, longitude, latitude, predictions.time_stamp, time_index FROM predictions, cluster_centers WHERE cluster_centers.cluster_id = predictions.cluster_id ')
        return array(c.fetchall()),None

    else: 
        return yp, y

if __name__ == '__main__':
    data = predict(45)
    from run_visualise import plot_trace
    da = zeros((10,2))
    da[:,0] = data[0:10,2]
    da[:,1] = data[0:10,1]
    print da
    plot_trace(da)


