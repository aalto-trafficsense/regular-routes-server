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

    print("Load model(s) from disk ...")
    import joblib
    model_minutes = [5,15,30]
    h = {}
    for i in model_minutes:
        h[i] = joblib.load("./pyfiles/prediction/dat/model_"+str(i)+"-"+str(DEV_ID)+".dat")

    ##################################################################################
    #
    # 2. Load trace (recent points) from database
    #
    ##################################################################################

    from .db_utils import get_conn, get_cursor

    conn = get_conn(use_test_server) 
    c = conn.cursor()

    print("Updating averaged location table")
    sql = open('./sql/update_averaged_location.sql', 'r').read()
    c.execute(sql)
    conn.commit()

    print("Extracting trace:")
    c.execute('SELECT hour,minute,day_of_week,longitude,latitude FROM averaged_location WHERE device_id = %s ORDER BY time_stamp DESC limit 10', (str(DEV_ID),))
    dat = array(c.fetchall(),dtype={'names':['H', 'M', 'DoW', 'lon', 'lat'], 'formats':['f4', 'f4', 'i4', 'f4','f4']})
    X = flipud(column_stack([dat['lon'],dat['lat'],dat['H']+(dat['M']/60.),dat['DoW']])) # <--- the flipup function is important to restor time ordering

    print(X)

    ##################################################################################
    #
    # 3. Filtering
    #
    ##################################################################################
    print("Filtering ...")

    from .pred_utils import do_feature_filtering

    Z = do_feature_filtering(X)
    print("... filtered to ", len(Z), "points")

    ##################################################################################
    #
    # 4. Prediction
    #
    ##################################################################################

    print("Prediction(s) (node): ", end=' ')

    yp = {} # store predictions
    py = {} # store confidences on the predictions
    z = Z[-1].reshape(1,-1) # instance

    for i in model_minutes:
        yp[i] = h[i].predict(z).astype(int)[0]
        py[i] = max(h[i].predict_proba(z)[0])
    print(yp, py)

    ##################################################################################
    #
    # 6. Form Geojson
    #
    ##################################################################################

    c.execute('SELECT max(time_stamp) as current FROM averaged_location WHERE device_id = %s', (str(DEV_ID),))
    current = c.fetchall()[0][0]

    print("Getting coordinates for prediction (from cluster_centers table), turning into geojson ... ", end=' ') 
    import json

    features = []
    for i in model_minutes:

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
                    "title": "node prediction "+str(i)+" minute/s from now ("+str(current)+"), at "+str(py[i])+"% confidence.",
                    "time": str(current),
                    "minutes": i,
                    "node_id": yp[i],
                    "confidence": py[i]
                }
            })

    c.execute("SELECT ST_AsGeoJSON(ST_MakePoint(%s, %s)), NOW()", (z[0,2],z[0,1],))
    row = c.fetchall()[0]
    features.append({
        'type': 'Feature',
        'geometry': json.loads(row[0]),
        'properties': {
            "type": "Position",
            "activity": "UNSPECIFIED",
            "title": "current location (at "+str(current)+")",
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

    print("Commit prediction")
    sql = "INSERT INTO predictions (device_id, cluster_id, time_stamp, time_index) VALUES (%s, %s, NOW(), %s)"
    for i in model_minutes:
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

    DEV_ID = 45
    use_test_server = False

    import sys

    if len(sys.argv) > 2:
        use_test_server = (sys.argv[2] == 'test')
    if len(sys.argv) > 1:
        DEV_ID = int(sys.argv[1])
        print(str(predict(DEV_ID,use_test_server)))
    else: 
        print("""Use: python run_prediction.py <DEV_ID> [test]
    where 'test' indicates to use the test server, and DEV_ID is the device ID,
       e.g., python run_prediction.py 45 test
       e.g., python run_prediction.py 45""")




