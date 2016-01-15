#!/usr/bin/python

"""
    Testing the database connection
    -------------------------------
    Connect to the surver and request a list of device IDs
"""

from numpy import *
set_printoptions(precision=5, suppress=True)

from db_utils import get_cursor

def dev_ids(cur):

    try:
        cur.execute("""SELECT DISTINCT device_id from device_data""")
    except:
        return "I can't SELECT from device_data"

    return cur.fetchall()

def num_trace(cur, d_id):

    try:
        cur.execute("""SELECT COUNT(*) from averaged_location WHERE device_id = %s """, (str(d_id),))
    except:
        return "I can't SELECT from device_data"

    return cur.fetchall()[0]


cur = get_cursor(False)
IDs = dev_ids(cur)
print str(IDs)

for i in IDs:
    i_d = i[0]
    c = num_trace(cur,i_d)[0]
    print "[%d] trace: %d" % (i_d, c)

