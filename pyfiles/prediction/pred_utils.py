from numpy import *

from FF import FF

def cdistance2metres(p1,p2):
    from geopy.distance import vincenty
    return vincenty(p1, p2).meters


def do_cluster(X, N_clusters=10):

    """
        CLUSTERING: Create N_clusters clusters from the data
        ----------------------------------------
    """
    from sklearn.cluster import KMeans
    print "Clustering", len(X),"points into", N_clusters, "personal nodes"
    h = KMeans(N_clusters, max_iter=100, n_init=1)
    h.fit(X[:,0:2])
    labels = h.labels_
    nodes = h.cluster_centers_
    return nodes


def do_snapping(X, nodes):
    """
        SNAPPING: snap all lon/lat points in X to a cluster, return as Y.  
        -----------------------------------------------------------------
        NOTE: it would also be a possibility to create and use an extra column in X (instead of Y).
    """
    print "Snapping trace to these ", len(nodes)," waypoints"
    from utils import snap
    print X.shape,nodes.shape
    Y = snap(X[:,0:2],nodes).astype(int)

    return Y

def do_movement_filtering(X,min_metres,b=10):
    """
        FILTERING: Filter out boring examples
        --------------------------------------
        We don't want to waste time and computational resources training on stationary segments.
        (Also for a demo we don't want to watch an animation where nothing is animated).
    """

    print "Filter out stationary segments ...", 

    T,D = X.shape

    X_ = zeros(X.shape)
    X_[0:b,:] = X[0:b,:]
    i = b
    breaks = [0]
    for t in range(b,T):

        xx = X[t-b:t,0:2]
        # 1. get lat, lon distance
        # 2. convert to metres
        p1 = array([min(xx[:,0]), max(xx[:,1])])
        p2 = array([max(xx[:,0]), max(xx[:,1])])
        # 3. calc distance
        d = cdistance2metres(p1,p2)
        # 4. threshold
        if d > min_metres:
            #print i,"<-",t
            X_[i,:] = X[t,:]
            i = i + 1
        else:
            breaks = breaks + [t]

    X = X_[0:i,:]
    T,D = X.shape
    print "... We filtered down from", T, "examples to", i, "consisting of around ", (T*100./(len(breaks)+T)), "% travelling."
    return X


def do_feature_filtering(X):
    """
        Turn raw data `X` into more advanced features, as `Z`.
        ------------------------------------------------------
        See `FF.py` on how this works.
        Note: most of the predictive power comes from good features!!!
    """

    #print "Pass thorugh ESN filter"
    T,D = X.shape
    #from sklearn.kernel_approximation import RBFSampler
    #rbf = RBFSampler(gamma=1, random_state=1)
    #H=D*2+1 #20
    rtf = FF(D)
    H = rtf.N_h
    #X = cc.coord_center(X)  # CENTER DATA
    Z = zeros((T,H))
    for t in range(0,T):
        #print X[t,0:2], Y[t+1]
        Z[t] = rtf.phi(X[t])

    #print "... turned ", X.shape, "into", Z.shape

    return Z

