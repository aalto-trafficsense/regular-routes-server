#!/usr/bin/python

'''
    Run Prediction
    ----------------------------------

    Obtain a prediction for the specified device ID.

    Note: when running ./run_prediction.py <ID>, it looks for the model file in 
    ```
    ./pyfiles/prediction/dat/model-<ID>.dat
    ```
'''

# Scientific Libraries
from numpy import *
set_printoptions(precision=5, suppress=True)

def predict(DEV_ID,use_test_server=False):
    '''
        1. load model from disk
        2. obtain a recent part of the trace
        3. filter it
        4. make a prediction 
        5. commit the prediction to the database
        6. return the prediction as geojson
    '''

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

    print "Extracting trace:"
    c.execute('SELECT hour,minute,day_of_week,longitude,latitude FROM averaged_location WHERE device_id = %s ORDER BY time_stamp DESC limit 10', (str(DEV_ID),))
    dat = array(c.fetchall(),dtype={'names':['H', 'M', 'DoW', 'lon', 'lat'], 'formats':['f4', 'f4', 'i4', 'f4','f4']})
    X = flipud(column_stack([dat['lon'],dat['lat'],dat['H']+(dat['M']/60.),dat['DoW']])) # <--- the flipup function is important to restor time ordering

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

    print "Prediction (node): " ,

    yp = h.predict(Z[-1].reshape(1,-1)).astype(int)[0]
    print yp

    print "Getting coordinates for prediction (from cluster_centers table): ", 
    #c.execute('SELECT longitude,latitude FROM cluster_centers WHERE device_id = %s AND cluster_id = %s', (DEV_ID,yp,))
    #dat = array(c.fetchall(),dtype={'names':['lon', 'lat'], 'formats':['f4', 'f4']})[0]
    #c.execute('SELECT ST_MakePoint(longitude, latitude) FROM cluster_centers WHERE device_id = %s AND cluster_id = %s', (DEV_ID,yp,))
    import json
    c.execute('SELECT ST_AsGeoJSON(location) FROM cluster_centers WHERE device_id = %s AND cluster_id = %s', (DEV_ID,yp,))
    dat = c.fetchall()[0]
    #dat = json.loads(row)
    #print c.fetchall()[0]
    #exit(1)
    #dat = json.loads(''+c.fetchall()[0])
    #print dat
    #exit(1)
    #current = snap(X[-1,0:2],nodes).astype(int)

    ##################################################################################
    #
    # 5. Commit prediction
    #
    ##################################################################################

    print "Commit prediction"
    sql = "INSERT INTO predictions (device_id, cluster_id, time_stamp, time_index) VALUES (%s, %s, NOW(), %s)"
    c.execute(sql, (DEV_ID, yp, 1,))
    conn.commit()

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

    ##################################################################################
    #
    # 6. Form Geojson
    #
    ##################################################################################

    print "Form geojson, and return it ..."

    return {
    'features': [
        {
            'geometry': {
                'coordinates': dat, #[
                    #dat[1], 
                    #dat[0]
                #],
                'type': 'Point',
            },
            'properties': {
                'type': 'Prediction',
                'activity': 'UNSPECIFIED',
                'predtype': 'node-prediction at 1 minute from now',
                'node_id': yp
            },
            'type': 'Feature',
        },
    ],
    'type': 'FeatureCollection'
    }

if __name__ == '__main__':
    print str(predict(45,use_test_server=True))


