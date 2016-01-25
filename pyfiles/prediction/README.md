# Documentation

## Requirements

Using `Python 2.7`, the following libraries are most essential (their dependencies are not included on the list):

* `psycopg2` to connect to the database
* `numpy` for vectors and matrices
* `sklearn` for clustering and learning (building models)
* `joblib` saving a model to disk
* `geopy.distance` for measuring distances between coordinates

For the demo, additionally,

* `smopy` for obtaining OSM data (mainly for getting andplotting ontop of a map)
* `matplotlib` for plotting and saving animations (as MP4 files)

To run the scripts for testing purposes, they can be run locally, but first connect to a server, possibly tunnelling through, e.g., 

```sh
laptop ~ $ ssh -L 5432:localhost:54322 <username>@kekkonen.niksula.hut.fi
kekkonen ~ $ ssh -L 54322:localhost:5432 <username>@regularroutes.niksula.hut.fi
```

which will allow access to the database.  Scripts can be run like
```sh
$ python run_build_models.py 98
```

There's a difference between the test server and the non-test server!

For build models and making predictions from the API, see the calls in `devserver.py`.


## Description of files

* Files beginning with `run_` can be run on the command line (useful for testing!), but also contain methods that can be called from the server api.
* Files with `_utils` in the name contain utility methods (some are probably not used anymore)
* `FF.py` contains the feature-function filter

## Building Models (`run_build_models.py`)

1. build and extract the 'averaged location' trace in/from the database
2. filter trace 
3. cluster points (and save to database)
4. build model(s)
5. dump model to disk

## Making Predictions (`run_prediction.py`)

1. load model from disk
2. update and select the recent 'averaged location'
3. filter the segment (feature-filter only)
4. make a prediction
5. match the prediction (node ID) to node coordinates using database; form a geojson string with the result, and return it after the following step
6. commit the prediction to the database 

## Demo (`run_demo.py`)

Then simply run `python run_demo.py <deviceID> [test]` where `test` specifies to use the test server. For example

```sh
$ python run_demo.py 98
```

It does the following:

1. Get OSM data (using `smopy` library) and a png image for the specified bounding box
2. [Set up plots, animation]
3. Connect to database, create and fill `averaged_location` table (run `make_average_table.sql`)
4. Load trace from the database into a an array, 
	```
	SELECT hour,minute,day_of_week,longitude,latitude FROM averaged_location ...

	```

	and create a `numpy` array with four columns (lat,lon,time,day):
	```python
		X = column_stack([dat['Lon'],dat['Lat'],dat['H']+(dat['M']/60.),dat['DoW']])
	```

5. Process a chunk of data (points `X[0:t]`, up to point `t`):
	* create some **personal nodes**

		```python
		from sklearn.cluster import KMeans
		h = KMeans(k, max_iter=100, n_init=1)
		h.fit(X[:,0:2])
		nodes = h.cluster_centers_
		```
    * **snap** averaged location points to personal nodes
	* **filter** out boring segments (non-movement)
	* create advanced recurrent features (**feature filter** `FF.py`)
	* return this data as `Z[0:t]`	

6. Build models on data `Z[0:t]`, e.g., learn to predict one step ahead

	```python
	from sklearn.ensemble import RandomForestClassifier
	h = RandomForestClassifier(n_estimators=100)
    h.fit(Z[0:t-1],Z[1:t,c]) # c is a column where the snapped personal node ID is stored
	```

7. `While t < T:`
    * `t = t + 1`
	* make predictions, e.g.,
	```python
            predicted_next_node = h.predict(Z[t]) # predict personal node, at time `t+1`
	```
	* [update on plot, save frame]
	* if the hour cell in `Z[t]` corresponds to the end of the day, GOTO 5.
8. [Export animation as `mp4`]

## TODO (Unfinished and Future development)

Low Hanging Fruit

- [ ] Improve/add more information (time of prediction, expected time of arrival, etc) in the geojson string return from `predict`.
- [ ] Use `user_id` instead of `device id` throughout
- [ ] Output different forms of predictions (e.g., 5-min, 20-min destination, minute-by-minute route prediction as in the demo)
- [ ] Fade out nodes over time (i.e., remove non-regular routes)
- [ ] Use 'crowd nodes' instead of personal nodes
- [ ] Analysis of a decision tree model used in prediction, to see how the model is being created from the features
- [ ] Try different clustering methods, or try using the waypoints as clusters
- [ ] Improvement of the engineered/recurrent features in `FF.py` 
- [ ] Use the travel *mode* in the input 

Medium-Term

- [ ] Use travel *mode* in the _output_, and visualize it with a different colour
- [ ] Crowd prediction: display it along side ordinar predictions

Long Term

- [ ] Evaluate the worth of different features wrt their predictive power 

## Summary

This code has been simplified considerably, and most efforts were placed on getting a running system on a minimal set of standard libraries, suitable for server integration. 

Decision trees (and random forests thereof) are by far and away the most powerful learners. It appears most of the predictive power comes down to a few simply engineered features: knowing where the traveller is, which direction they are currently travelling in, and at what speed, and at what time of day and what day of the week.
