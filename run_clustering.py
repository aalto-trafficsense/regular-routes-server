#!/usr/bin/python2.4
#
# Small script to show PostgreSQL and Pyscopg together
#

from numpy import *
from sklearn.cluster import KMeans
import psycopg2

def cluster(device_id):
    d_id = str(device_id)

    try:
        conn = psycopg2.connect("dbname='regularroutes' user='regularroutes' host='localhost' password='TdwqStUh5ptMLeNl' port=5432")
    except:
        return "I am unable to connect to the database"

    c = conn.cursor()

    print "Test ..."
    c.execute('SELECT DISTINCT device_id FROM device_data')
    rows = c.fetchall()
    test = array(rows)
    #.fetchall()
    print test

    print "Drop cluster_centers Table...",
    c.execute('DELETE FROM cluster_centers')

#   print "Loading Device IDs ...",
#   c.execute('SELECT DISTINCT device_id FROM averaged_location')
#   rows = c.fetchall()
#   ids = array(rows ,dtype={'names':['d_id'], 'formats':['i4']})
#   print ids
#   for i in ids:
    #d_id = i[0]

    print "Loading Waypoints for Device ID _"+str(d_id)+"_ ..."
    c.execute('SELECT longitude,latitude,time_stamp,device_id FROM averaged_location WHERE device_id = %s', (str(d_id),))
    rows = c.fetchall()
    dat = array(rows, dtype={'names':['lon', 'lat', 't', 'd_id'], 'formats':['f4','f4','f4','i4']})
    X = column_stack([dat['lat'],dat['lon']])
    print "This device has", X.shape[0], "entries"
    
    if len(dat) < 50: 
        print "Not enough points for Clustering!"

    else:
        print "Clustering ..."
        h = KMeans(50, max_iter=100, n_init=1)
        h.fit(X)
        labels = h.labels_
        clusters = h.cluster_centers_
        print " ... made ", len(labels), "clusters."

        print "Inserting Cluster Centres ..."
        for k in range(50):
            sql = "INSERT INTO cluster_centers (longitude, latitude, location, cluster_id, device_id) VALUES (%s, %s, ST_MakePoint(%s,%s), %s, %s)"
            #print k, d_id, clusters[k,0], clusters[k,1], clusters.shape, labels[k]
            c.execute(sql, (str(clusters[k,0]), str(clusters[k,1]), str(clusters[k,0]), str(clusters[k,1]), str(k),  str(d_id)))

        print "Commit"
        conn.commit()


    return "Done! Clustered 50 nodes for device "+str(d_id)

if __name__ == '__main__':
    cluster(45)
