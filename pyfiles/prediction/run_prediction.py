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
        1. load model(s) from disk
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

    print "Load model(s) from disk ..."
    import joblib
    h = joblib.load("./pyfiles/prediction/dat/model-"+str(DEV_ID)+".dat")
    h5 = joblib.load("./pyfiles/prediction/dat/model_5-"+str(DEV_ID)+".dat")
    h30 = joblib.load("./pyfiles/prediction/dat/model_30-"+str(DEV_ID)+".dat")

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

    print "Prediction(s) (node): " ,

    yp = {} # store predictions
    py = {} # store confidences on the predictions
    z = Z[-1].reshape(1,-1) # instance
    yp[1] = h.predict(z).astype(int)[0]
    py[1] = max(h.predict_proba(z)[0])
    yp[5] = h5.predict(Z[-1].reshape(1,-1)).astype(int)[0]
    py[5] = max(h5.predict_proba(z)[0])
    yp[30] = h30.predict(Z[-1].reshape(1,-1)).astype(int)[0]
    py[30] = max(h30.predict_proba(z)[0])
    print yp, py

    ##################################################################################
    #
    # 6. Form Geojson
    #
    ##################################################################################


    print "Getting coordinates for prediction (from cluster_centers table), turning into geojson ... ", 
    import json

    features = []
    for i in [1,5,30]:

        sql = 'SELECT ST_AsGeoJSON(location),NOW() as time FROM cluster_centers WHERE device_id = %s AND cluster_id = %s'
        c.execute(sql, (DEV_ID,yp[i],))

        rows = c.fetchall()
        for row in rows:
            features.append({
                'type': 'Feature',
                'geometry': json.loads(row[0]),
                'properties': {
                    "type": "Prediction",
                    "activity": "UNSPECIFIED",
                    "title": "node prediction "+str(i)+" minute/s from now ("+str(row[1])+"), at "+str(py[i])+"% confidence.",
                    "time": str(row[1]),
                    "minutes": i,
                    "node_id": yp[i],
                    "confidence": py[i]
                }
            })

    geojson = {
        'type': 'FeatureCollection',
        'features': features
    }

    ##################################################################################
    #
    # 5. Commit prediction
    #
    ##################################################################################

    print "Commit prediction"
    sql = "INSERT INTO predictions (device_id, cluster_id, time_stamp, time_index) VALUES (%s, %s, NOW(), %s)"
    for i in [1,5,30]:
        c.execute(sql, (DEV_ID, yp[i], i,))
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

    #################################
    # RETURN 
    #################################

    #from flask import jsonify
    #return jsonify(geojson)
    return geojson

if __name__ == '__main__':
    print str(predict(45,use_test_server=True))


