#!/usr/bin/python

'''
    Run Prediction
    ----------------------------------

    Obtain a prediction for the specified device ID.
'''


# Scientific Libraries
from numpy import *
set_printoptions(precision=5, suppress=True)

def predict(DEV_ID,use_test_server=False,win_past=5,win_futr=5,mod="EML",lim=0,commit_result=True):
    '''
        1. obtain full trace AFTER 'lim' (or else the most recent readings)
        consider that points are new and haven't been snapped yet, so
            2. fetch clusters from cluster_centers
            3. snap full trace to clusters
        4. load a model from disk
        5. stack and TEST a model
    '''

    print "----------------\n\n\n---- BEGIN\n\n\n---------------------\n"

    ##################################################################################
    #
    # 1. Load model from disk
    #
    ##################################################################################

    print "Load model from disk ..."
    import joblib
    fname = "./pyfiles/prediction/dat/model-"+str(DEV_ID)+".dat"
    h = joblib.load( fname)

    ##################################################################################
    #
    # 2. Load trace (recent points) from database
    #
    ##################################################################################

    from db_utils import get_conn, get_cursor

    conn = get_conn(use_test_server) 
    c = conn.cursor()

    print "Updating averaged location table"
    sql = open('./sql/update_averaged_location.sql', 'r').read()
    c.execute(sql)
    conn.commit()

    print "Extracting trace"
    c.execute('SELECT hour,minute,day_of_week,longitude,latitude FROM averaged_location WHERE device_id = %s ORDER BY time_stamp DESC limit 10', (str(DEV_ID),))
    dat = array(c.fetchall(),dtype={'names':['H', 'M', 'DoW', 'lon', 'lat'], 'formats':['f4', 'f4', 'i4', 'f4','f4']})
    X = flipud(column_stack([dat['lon'],dat['lat'],dat['H']+(dat['M']/60.),dat['DoW']]))
    # <--- the flipup function is important to restor time ordering

    print "... Retrieved", len(X), "points:"
    print X

    ##################################################################################
    #
    # 3. Filtering
    #
    ##################################################################################
    print "Filtering ..."

    from pred_utils import do_feature_filtering

    Z = do_feature_filtering(X)
    print "... filtered to ", len(Z), "points"

    ##################################################################################
    #
    # 4. Prediction
    #
    ##################################################################################

    print "Prediction: " ,

    yp = h.predict(Z[-1].reshape(1,-1)).astype(int)[0]

    print "=========", yp

    return yp

#    if commit_result:
#
#        print "Committing to table ... " ,
#        for t in range(len(yp)):
#            sql = "INSERT INTO predictions (device_id, cluster_id, time_stamp, time_index) VALUES (%s, %s, NOW(), %s)"
#            c.execute(sql, (str(DEV_ID), str(yp[t]), str(t+1)))
#
#        conn.commit()
#
#        c.execute('SELECT cluster_centers.cluster_id, longitude, latitude, predictions.time_stamp, time_index FROM predictions, cluster_centers WHERE cluster_centers.cluster_id = predictions.cluster_id ')
#        return array(c.fetchall()),None
#
#    else: 
#        return yp, y

if __name__ == '__main__':
    data = predict(45,use_test_server=True)


