#!/usr/bin/python2.4

import psycopg2

from numpy import *

def run_clustering(db):
    #device_id int,
    #cluster_id bigint,
    #longitude float,
    #latitude float
    result = db.engine.execute("SELECT longitude, latitude FROM averaged_location")
    a = [r[0] for r in result]

    #dat = array(c.execute('SELECT lon,lat,t,d_id FROM data_averaged WHERE d_id=?', (45,)).fetchall(),dtype={'names':['lon', 'lat', 't', 'd_id'], 'formats':['f4','f4','f4','i4']})
    #X = column_stack([dat['lat'],dat['lon']])

    #from sklearn.cluster import KMeans
    #h = KMeans(50, max_iter=100, n_init=1)
    #h.fit(X)
    #labels = h.labels_

    count = 1
    db.engine.execute("INSERT INTO cluster_centers VALUES (0,0,1.1,1.2)")
    #count = 0
    #for i in range(len(h.cluster_centers_)):
    #    db.engine.execute(users_table.insert({'device_id': 0, 'cluster_id': 0, 'longitude': 1.1, 'latitude': 1.2}))
    #    count = count + 1
#    for row in result:
#            print "username:", row['username']
#
    return "inserted "+count+" rows into cluster_centers; "+str(a)
#dat = array(c.execute('SELECT lon,lat,t,d_id FROM data_averaged WHERE d_id=?', (45,)).fetchall(),dtype={'names':['lon', 'lat', 't', 'd_id'], 'formats':['f4','f4','f4','i4']})
#    with open('sql/learningtest.sql', 'r') as sql_file:
#        sql = sql_file.read()
#        result = db.engine.execute(text(sql))
