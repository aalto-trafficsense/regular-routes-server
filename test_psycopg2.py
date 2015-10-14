#!/usr/bin/python2.4
#
# Small script to show PostgreSQL and Pyscopg together
#

import psycopg2

def dev_ids():
	try:
	    conn = psycopg2.connect("dbname='regularroutes' user='regularroutes' host='localhost' password='TdwqStUh5ptMLeNl' port=5432")
	except:
	    return "I am unable to connect to the database"


	cur = conn.cursor()
	try:
	    cur.execute("""SELECT DISTINCT device_id from device_data LIMIT 10""")
	except:
	    return "I can't SELECT from device_data"

	rows = cur.fetchall()
	#print "\nRows: \n"
	#for row in rows:
	#    print "   ", row

	return "DEVICE IDs (limit 10): "+str(rows)

print dev_ids()
