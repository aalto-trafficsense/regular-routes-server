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

##################################################################################
#
# Options       
#
##################################################################################

DEV_ID = 77
if len(sys.argv) > 3:
    DEV_ID = int(sys.argv[3])

# PREDICTION WINDOWS (PAST/FUTURE)
win_past = 5
win_futr = 5
if len(sys.argv) > 1:
    win_past = int(sys.argv[1])
if len(sys.argv) > 2:
    win_futr = int(sys.argv[2])

##################################################################################
#
# Load trace
#
##################################################################################
import psycopg2

try:
    conn = psycopg2.connect("dbname='regularroutes' user='regularroutes' host='localhost' password='TdwqStUh5ptMLeNl' port=5432")
except:
    print "I am unable to connect to the database"

c = conn.cursor()

print "Extracting trace"
c.execute('SELECT device_id,hour,day_of_week,longitude,latitude FROM averaged_location WHERE device_id = %s ORDER BY time_stamp DESC LIMIT 100', (str(DEV_ID),))
dat = array(c.fetchall(),dtype={'names':['d_id', 'H', 'DoW', 'lon', 'lat'], 'formats':['i4', 'i4', 'i4', 'f4','f4']})
run = column_stack([dat['lon'],dat['lat']])
X = column_stack([dat['lon'],dat['lat'],dat['H'],dat['DoW']])
print X

##################################################################################
#
# Extract nodes
#
##################################################################################

print "Extracting waypoints"
c.execute('SELECT latitude, longitude FROM cluster_centers WHERE device_id = %s', (str(DEV_ID),))
rows = c.fetchall()
nodes = array(rows)

#for i in range(len(nodes)):
#    print i, nodes[i]
##################################################################################
#
# Snapping
#
##################################################################################
print "Snapping to ", len(nodes)," waypoints"
y = snap(run,nodes)

print y

print "Stack into ML dataset ..."
X,Y = stack(X,y,win_past,win_futr)

#print X.shape
#print Y.shape

print "Load model ..."
from sklearn import ensemble
from ML import *

import joblib
fname = "./dat/model_dev"+str(DEV_ID)+".model"
h = joblib.load( fname)

print X.shape
x = X[-1,:]
print "x=", x.shape

print "Prediction: " ,

yp = h.predict(array([x]))[0]
print yp

print "Committing to table ... " ,
for t in range(len(yp)):
    sql = "INSERT INTO predictions (device_id, cluster_id, time_stamp, time_index) VALUES (%s, %s, NOW(), %s)"
    c.execute(sql, (str(DEV_ID), str(yp[t]), str(t+1)))

conn.commit()
