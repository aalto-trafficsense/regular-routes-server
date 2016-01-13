# Documentation

## List of technologies/libraries used

* Python 2
* `smopy` for obtaining OSM data (mainly for plotting, and measuring distances)
* `psycopg2` to connect to the database
* `numpy` for vectors and matrices
* `sklearn` for clustering and learning
* `matplotlib` for plotting and saving animations (as mp4 files)

## Demo `run_demo.py` for a particular device ID

Simply run `python run_demo.py <deviceID> [test]` where `test` specifies to use the test server. It does the following:

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

