#!/usr/bin/python

'''
    
    Build Models
    ----------------------------------

    Running 
    ```
        train(98) should do the following ...
    ```
'''

# Scientific Libraries
from numpy import *
set_printoptions(precision=5, suppress=True)

# Provides snapping and stacking functionality
import sys
sys.path.append("./src")

def train(DEV_ID,use_test_server=False,win_past=5,win_futr=5,mod="EML",lim='NOW()'):
    '''
        TRAIN A MODEL, DUMP IT TO DISK
        -------------------------------
        1. obtain full trace PRIOR OR EQUAL TO 'lim'
        2. filter
        3. cluster and snap full trace to clusters
        4. build model
        5. dump the model to disk
    '''

    print "----------------\n\n\n---- BEGIN\n\n\n---------------------\n"

    from db_utils import get_conn, get_cursor

    ##################################################################################
    #
    # 1. Load trace from database
    #
    ##################################################################################

    conn = get_conn(use_test_server) 
    c = conn.cursor()

    if not use_test_server:
        print "Building averaged_location table with new data."
        sql = open('../..//sql/make_average_table.sql', 'r').read()
        c.execute(sql)
        conn.commit()

    print "Extracting trace"
    c.execute('SELECT hour,minute,day_of_week,longitude,latitude FROM averaged_location WHERE device_id = %s', (str(DEV_ID),))
    dat = array(c.fetchall(),dtype={'names':['H', 'M', 'DoW', 'lon', 'lat'], 'formats':['f4', 'f4', 'i4', 'f4','f4']})
    X = column_stack([dat['lon'],dat['lat'],dat['H']+(dat['M']/60.),dat['DoW']])

    T,D = X.shape

    ##################################################################################
    #
    # 2. Filtering
    #
    ##################################################################################
    print "Filtering"

    from pred_utils import do_movement_filtering, do_feature_filtering

    X = do_movement_filtering(X,30) # to within 30 metres
    Z = do_feature_filtering(X)

    ##################################################################################
    #
    # 3. Get clusters (should be done separately, but we will do it 'manually' here.
    #
    ##################################################################################
    print "Clustering and Snapping"

    from pred_utils import do_cluster, do_snapping

    nodes = do_cluster(X)
    Y = do_snapping(X,nodes)

    ##################################################################################
    #
    # 4. Build Model
    #
    ##################################################################################
    print "Build Model"

    from sklearn.ensemble import RandomForestClassifier

    h = RandomForestClassifier(n_estimators=100)
    h.fit(Z[0:-1],Y[1:])   

    ##################################################################################
    #
    # 5. Dump to Disk
    #
    ##################################################################################
    print "Dump to Disk"

    import joblib
    joblib.dump(h,  './dat/model-'+str(DEV_ID)+'.dat')

    return "OK! "+str(DEV_ID)+" Successfully built!"


if __name__ == '__main__':
    train(98)

