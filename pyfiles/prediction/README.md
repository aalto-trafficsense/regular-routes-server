# Documentation

## Technologies/libraries used

* Python 2.7
* `smopy` for obtaining OSM data (mainly for plotting, and measuring distances)
* `psycopg2` to connect to the database
* `numpy` for vectors and matrices
* `sklearn` for clustering and learning
* `matplotlib` for plotting and saving animations (as mp4 files)

## Demo 

To run the demo, first simply connect to a server, possibly tunnelling through, e.g., 

```sh
laptop ~ $ ssh -L 5432:localhost:54322 <username>@kekkonen.niksula.hut.fi
kekkonen ~ $ ssh -L 54322:localhost:5432 <username>@regularroutes.niksula.hut.fi
```

then simply run `python run_demo.py <deviceID> [test]` where `test` specifies to use the test server. For example

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

## Server code to run nightly

Significant development on prediction and data processing has rendered most of the other `run_` files outdated. However, I have since updated `run_build_models.py` which carries out most of the same process as `run_demo.py` (in fact it uses much of the same code) but instead dumps a model to disk (in the `dat` folder). As of writing, 

In other words, running
```sh
$ python run_build_models.py 98
```
will fetch all data for device `98`, cluster, filter, build the model and save it to `./dat/model-98.npy`.

## TODO (Unfinished and Future development)

- [ ] finish integration to the server database by creating `run_prediction.py`
- [ ] clustering could possibly be separated into `run_clustering.py`
- [ ] use user name instead of device id
- [ ] global nodes instead of personal nodes
- [ ] fade out nodes over time (i.e., remove non-regular routes)
- [ ] analyse one of the decision tree models to see how the model is being created from the features
- [ ] use the travel MODE both in the input (easy) and for output (not as easy)
