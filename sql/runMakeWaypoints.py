import sqlite3
from numpy import *
conn = sqlite3.connect('../../../../Workbox/routes.db')
c = conn.cursor()
dat = array(c.execute('SELECT lon,lat,t,d_id FROM data_averaged WHERE d_id=?', (45,)).fetchall(),dtype={'names':['lon', 'lat', 't', 'd_id'], 'formats':['f4','f4','f4','i4']})
X = column_stack([dat['lat'],dat['lon']])
print "DONE"

from sklearn.cluster import KMeans
h = KMeans(50, max_iter=100, n_init=1)
h.fit(X)
labels = h.labels_
print "DONE"

c.execute("DROP TABLE IF EXISTS nodes;")
c.execute("CREATE TABLE nodes (d_id int, n_id, lon float, lat float);")
for i in range(len(h.cluster_centers_)):
    print "insert", i
    c.execute("INSERT INTO nodes values (?,?,?,?)", (45, i, h.cluster_centers_[i,1], h.cluster_centers_[i,0]))
print "commit"
conn.commit()

from matplotlib.pyplot import *
figure()
MIN_N = 60.15
LIM_N = 60.24
MIN_E = 24.68
LIM_E = 24.98
ylim(MIN_N,LIM_N)   # plot x from 0 to 10
xlim(MIN_E,LIM_E)   # plot y from 0 to 10
EXTENT = [MIN_E,LIM_E,MIN_N,LIM_N]
#scatter(X[:, 0], X[:, 1], c=labels.astype(np.float)*10)
print X
print h.cluster_centers_

FILE_MAP = './maps/map_dev45.png'
img = imread(FILE_MAP)
imshow(img, extent=EXTENT)
plot(h.cluster_centers_[:,0],h.cluster_centers_[:,1],'mo',markersize=10,linewidth=10)

print "DONE"
show()                               # display

exit(1)









from sklearn.cluster import DBSCAN
from sklearn import metrics
from sklearn.datasets.samples_generator import make_blobs
from sklearn.preprocessing import StandardScaler
# Compute DBSCAN
db = DBSCAN(eps=0.0010, min_samples=20).fit(X)
core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
core_samples_mask[db.core_sample_indices_] = True
labels = db.labels_

# Number of clusters in labels, ignoring noise if present.
n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)

# Plot result
import matplotlib.pyplot as plt

# Black removed and is used for noise instead.
unique_labels = set(labels)
colors = plt.cm.Spectral(np.linspace(0, 1, len(unique_labels)))
for k, col in zip(unique_labels, colors):
    if k == -1:
        # Black used for noise.
        col = 'k'

    class_member_mask = (labels == k)

    xy = X[class_member_mask & core_samples_mask]
    plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=col,
             markeredgecolor='k', markersize=14)

    xy = X[class_member_mask & ~core_samples_mask]
    plt.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=col,
             markeredgecolor='k', markersize=6)

plt.title('Estimated number of clusters: %d' % n_clusters_)
plt.show()

exit(1)


# TODO: PRODUCE TRAINING/TEST DATASET AS CSV (WITH WAYPOINTS)!@



# Update to database
#w_id = h.predict(X)
for i in range(len(dat['t'])):
    print "UPDATE data_averaged SET w_id = ? WHERE t = ? and d_id = 45", h.predict(X[i,:])[0], dat['t'][i]
    c.execute("UPDATE data_averaged SET w_id = ? WHERE t = ? and d_id = 45", ( str(h.predict(X[i,:])[0]), str(dat['t'][i]) ))
    #c.execute("UPDATE data_averaged SET w_id = ? WHERE d_id = 45", ( str(h.predict(X[i,:])[0]), ))

#c.executemany("UPDATE data_averaged SET w_id = ? WHERE t = ? and d_id = 45", ( h.predict(X), dat['t'] ))
#my_data = ({id=1, value='foo'}, {id=2, value='bar'})
#cursor.executemany('UPDATE test SET myCol=:value WHERE rowId=:id', my_data)
#c.commit()
