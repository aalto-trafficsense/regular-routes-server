#!/usr/bin/python

# Scientific libraries
from numpy import *
random.seed(0)
set_printoptions(precision=5, suppress=True)

# Prepare for plotting
import matplotlib
matplotlib.use('Qt4Agg')
from matplotlib.pyplot import *

# Other necessary libraries
import copy

##################################################################################
#
# Important Functions
#
##################################################################################

def center(x,min_x,max_x):
    return (x - min_x) / (max_x - min_x)

def uncenter(x,min_x,max_x):
    return x * ( max_x - min_x ) + min_x

def coord_center(xy, mins, maxs):
    c_xy = zeros(2)
    for j in range(2):
        c_xy[j] = center(xy[j],mins[j],maxs[j])
    return c_xy

def t2hr(tt):
    print tt
    hh = int(tt * 24.)
    mm = int(((tt * 24) - hh) * 60)
    return "%02dh%02d" % (hh,mm)

def tick(z, minutes):
    return z + minutes/24./60.

from db_utils import get_cursor

##################################################################################
#
# Parameters
#
##################################################################################

DEV_ID = 45
import sys
if len(sys.argv) > 1:
    DEV_ID = int(sys.argv[1])

b = 10              # majic window parameter
thres = 0.001       # relative movement threshold for not-filtering
T_p = 10       # how far to predict into the future

days = "Sun Mon Tue Wed Thu Fri Sat".split(" ")
timer = 0

##################################################################################
#
# Load trace
#
##################################################################################

X = None

import os.path
FILE_X = './dat/'+str(DEV_ID)+'_stream_X.csv'
 
if not os.path.isfile(FILE_X):

    c = get_cursor(True) 

    print "Extracting trace"
    c.execute('SELECT hour,minute,day_of_week,longitude,latitude FROM averaged_location WHERE device_id = %s', (str(DEV_ID),))
    dat = array(c.fetchall(),dtype={'names':['H', 'M', 'DoW', 'lon', 'lat'], 'formats':['f4', 'f4', 'i4', 'f4','f4']})
    X = column_stack([dat['lon'],dat['lat'],dat['H']+(dat['M']/60.),dat['DoW']])
    savetxt(FILE_X, X, delimiter=',')

X = genfromtxt(FILE_X, skip_header=0, delimiter=',')

##################################################################################
#
# Load clusters and Snap to Them
#
##################################################################################

FILE_Y = './dat/'+str(DEV_ID)+'_stream_Y.csv'
FILE_N = './dat/'+str(DEV_ID)+'_stream_nodes.csv'

if not os.path.isfile(FILE_Y) or not os.path.isfile(FILE_N):

    c = get_cursor(True) 

    print "Extracting waypoints"
    c.execute('SELECT latitude, longitude FROM cluster_centers WHERE device_id = %s', (str(DEV_ID),))
    rows = c.fetchall()
    nodes = array(rows)
    if len(nodes) <= 0:
        print "[WARNING] No nodes available, we will create them now ..."
        from run_clustering import cluster
        cluster(DEV_ID)
        c.execute('SELECT latitude, longitude FROM cluster_centers WHERE device_id = %s', (str(DEV_ID),))
        rows = c.fetchall()
        nodes = array(rows)
        print "... created ", len(nodes)," waypoints"

    print "Snapping trace to these ", len(nodes)," waypoints"
    import sys
    sys.path.append("src/")
    from utils import snap
    print X.shape,nodes.shape
    Y = snap(X[:,0:2],nodes).astype(int)

    savetxt(FILE_N, nodes, delimiter=',')
    savetxt(FILE_Y, Y, delimiter=',')

nodes = genfromtxt(FILE_N, skip_header=0, delimiter=',')
Y = genfromtxt(FILE_Y, skip_header=0, delimiter=',')

##################################################################################
#
# Load Map
#
##################################################################################

bx = (60.1442, 24.6351, 60.3190, 25.1741)   # Helsinki 1
#bx = (min(nodes[:,0]), min(nodes[:,1]), max(nodes[:,0]), max(nodes[:,1]))
print "Get Map"
import smopy
import joblib
FILE_M = './dat/Helsinki.dat' #'+str(DEV_ID)+'_
FILE_I = './dat/Helsinki.png' #'+str(DEV_ID)+'_
if not os.path.isfile(FILE_M):
    print "Creating Map..."
    map = smopy.Map(bx,z=12)
    print "Saving Map..."
    joblib.dump(map,  FILE_M)
    print "Saving Map Image ..."
    map.save_png(FILE_I)
map = joblib.load(FILE_M)

##################################################################################
#
# Transforming data
#
##################################################################################

mins = zeros(4)
maxs = zeros(4)

for j in range(4):
    mins[j] = min(X[:,j])
    maxs[j] = max(X[:,j])

# Center the data
X_raw = ones(X.shape) * X
for j in range(4):
    X[:,j] = center(X[:,j],mins[j],maxs[j])

def scaled2pixels(cx,cy,lx,ly,bx,ucen=True):
    '''
        cx = 0.1
        lx = 1792
        mins = 60.011
        bx = 60.1442 
    '''
    if ucen:
        # turn back into lat/lon coordinates first
        cx = uncenter(cx,mins[0],maxs[0])
        cy = uncenter(cy,mins[1],maxs[1])
    px = center(cx,bx[0],bx[2]) 
    py = center(cy,bx[1],bx[3]) 
    return px * lx, (-py*ly + img.shape[0] )

##################################################################################
#
# Filtering the data
#
##################################################################################


# Filter out boring examples
print "Filter ...", 

T,D = X.shape

X_ = zeros(X.shape)
X_[0:b,:] = X[0:b,:]
y_ = zeros(Y.shape)
y_[:] = Y[:]
i = b
for t in range(b,T):
    ee = X[t-b:t-1,0:2] - X[t,0:2]
    d = sqrt(max(ee[:,0])**2 + max(ee[:,1])**2)
    if d > thres:
        X_[i,:] = X[t,:]
        y_[i] = Y[t]
        i = i + 1
X = X_[0:i,:]
Y = y_[0:i]
print "... from ", T, "examples to", i

T,D = X.shape

FILE_F = './dat/'+str(DEV_ID)+'_stream_F.csv'
if not os.path.isfile(FILE_F):
    print "Saving filtered data (could use this directly in the future)"
    savetxt(FILE_F, X, delimiter=',')

# Pass through an ESN filter
print "Pass thorugh ESN filter"
import sys
#from sklearn.kernel_approximation import RBFSampler
#rbf = RBFSampler(gamma=1, random_state=1)
from FF import FF
#H=D*2+1 #20
H = 11
rtf = FF(D,10)
Z = zeros((T,H))
for t in range(T):
    #print X[t,0:2], Y[t+1]
    Z[t] = rtf.phi(X[t])

print "... turned ", X.shape, "into", Z.shape

##################################################################################
#
# Machine Learning and Plotting
#
##################################################################################

ion()

print "Setup"

# Multi-output Regressior
from sklearn.linear_model import SGDRegressor, SGDClassifier
from sklearn.neighbors import KNeighborsRegressor
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn import tree
h = SVC()
h = KNeighborsClassifier(n_neighbors=10)
h = SGDClassifier()
from sklearn.ensemble import RandomForestClassifier
#h = tree.DecisionTreeClassifier()
h = RandomForestClassifier(n_estimators=100)
#print Z[0:T-1].shape
#print Y[1:T].shape
print "Training initial chuck #1"
t_0 = int(T * 0.88)      # initial chunk of training
h.fit(Z[0:t_0-1],Y[1:t_0])       # <--- train on a significant chunk at a time (sklearn's tree not incremental)
#h.fit(Z[0:b],Y[1:b+1])
#h.partial_fit(Z[0:T-1],Y[1:T],classes=range(len(nodes)))
#h.partial_fit(Z[0:b],X[1:b+1,0:2])
print "Training initial chuck #2"
#g = tree.DecisionTreeClassifier()
g = RandomForestClassifier(n_estimators=100)
g.fit(Z[0:t_0-10],Y[10:t_0])     # <--- train on a significant chunk at a time (sklearn's tree not incremental)

# With init. batch of b, until T.
window = zeros(b)
history = zeros((T))

fig = figure()
from matplotlib import gridspec
#gs = gridspec.GridSpec(2, 1, height_ratios=[1, 6]) 
gs = gridspec.GridSpec(1, 1)
'''
ax0 = fig.add_subplot(gs[0])
l, = ax0.plot(0,0,'k-',markersize=10,linewidth=1)
grid(True)
ax0.set_title("error / "+str(len(X)))
'''

ax1 = fig.add_subplot(gs[0]) # was: [1]
ax1.set_title("Map, device ID "+str(DEV_ID))
img = imread(FILE_I) #
ax1.imshow(img)
ax1.set_xlim([0,img.shape[1]]) #
ax1.set_ylim([img.shape[0],0]) #
#ax1.set_xlim([-0.1,+1.1])
#ax1.set_ylim([-0.1,+1.1])
node_pxs = map.to_pixels(nodes)
n_, = ax1.plot(node_pxs[:,0],node_pxs[:,1],'bo',markersize=5,label="node")
mg, = ax1.plot(0,0,'mo',markersize=10,linewidth=3,label="10-min destination pred.")
mp, = ax1.plot(0,0,'mo-',markersize=2,linewidth=4,label="5-min route prediction")
m, = ax1.plot(0,0,'r-',markersize=1,linewidth=3,label="recent trajectory (true)")
mm, = ax1.plot(0,0,'ro',markersize=6,linewidth=3,label="true position")
ax1.set_yticklabels([])
ax1.set_xticklabels([])
ax1.grid(False)
msg = "Hey device %d, if you stop at shop X\n,in the next 30 minutes you get\n 10%% discount. Discount code: 123" % (DEV_ID)
tx1 = ax1.text(img.shape[1]/2, img.shape[0]/2, msg, style='italic', fontsize=15, bbox={'facecolor':'red', 'alpha':0.5, 'pad':10})
tx1.set_visible(False)
tx2 = ax1.text(img.shape[1]/2, img.shape[0]/2, "UPDATING", fontsize=25, bbox={'facecolor':'blue', 'pad':10})
tx2.set_visible(False)
gs.tight_layout(fig,h_pad=0.1)
#fig.tight_layout()
legend()

#figManager = get_current_fig_manager()
#figManager.window.showMaximized()
show()

print "Go"
yp = 0
images = []

import matplotlib.animation as animation
FFMpegWriter = animation.writers['ffmpeg']
metadata = dict(title='Simulation Device '+str(DEV_ID), artist='Jesse', comment='Demo')
writer = FFMpegWriter(fps=15, bitrate=5000, metadata=metadata)
with writer.saving(fig, "dat/Regular_Routes_OLD_Dev_"+str(DEV_ID)+".mp4", 100):
#if True:
    for t in range(t_0,T-1):

        #######################
        # Plot the trace
        #######################
        t0 = max(0,t-b)
        XX = array(map.to_pixels(uncenter(X[t0:t,0],mins[0],maxs[0]),uncenter(X[t0:t,1],mins[1],maxs[1]))).T
        mm.set_xdata(XX[-1,0])
        mm.set_ydata(XX[-1,1])
        m.set_xdata(XX[:,0])
        m.set_ydata(XX[:,1])

        #######################
        # Predict into the future, P(y[t+1]|X[t])
        #######################
        yp = h.predict(Z[t].reshape(1,-1)).astype(int)
        #print "[t=%d, DOW=%d]" % (t,X[t,3]*7.),
        #print yp,
        yp_g = g.predict(Z[t].reshape(1,-1)).astype(int)
        gnp = node_pxs[yp_g]
        mg.set_xdata(gnp[:,0])
        mg.set_ydata(gnp[:,1])
        mg.set_data(gnp[0])
        if yp_g == 3 and timer == 0:
            tx1.set_visible(True)
            pause(5)
            tx1.set_visible(False)
            timer = 100

        #######################
        # Predict a full trace
        #######################
        YP = zeros((T_p,2))         # we will predict T_p steps ahead
        YP[0] = XX[-1,:]            # end of present trace
        #rtf2 = copy.deepcopy(rtf)
        rtf2 = FF(D,10)             # make a new filter
        rtf2.Z[0] = Z[t][:]         # prime it
        xp = copy.deepcopy(X[t+1])
        for i in range(1,T_p):
            #zp = xp
            zp = rtf2.phi(xp[:])
            ypp = h.predict(zp.reshape(1,-1)).astype(int)[0]       # predict coordinates
            print ypp,
            #print "%d = argmax_y p(y[%d] | %s) " % (ypp, i, zp)
            #print "nodes: ", nodes[ypp]                            # GEO-coords
            YP[i] = node_pxs[ypp]                                  # MIG-coords
            cpp = coord_center(nodes[ypp],mins,maxs)               # ML-coords
            #print "codes: ", cpp
            xp[0:2] = cpp
            xp[2] = tick(xp[2],5.) #+ 1.#0.01 #33

        #print ""

        mp.set_xdata(YP[:,0])
        mp.set_ydata(YP[:,1])

        #######################
        # Evaluate and plot the error
        #######################
        b_ = t % b
        #window[b_] = sqrt((Y[t] - yp)**2)
        window[b_] = (Y[t] == yp)*1.
        history[t-b] = mean(window)
        #print "AVG", history[t-b], Y[t], " LEARN:", array([Z[t]]), "->", X[t+1,0:2], Y[t]
        '''
        l.set_data(range(0,t-b),history[0:t-b])
        ax0.set_xlim([0,t-b])
        ax0.set_ylim([0,max(history)])
        '''

        #######################
        # Update classifier
        #######################
        #h.partial_fit(array([Z[t-1]]),X[t+1,0:2].reshape(1,-1))
        #h.partial_fit(array([Z[t-1]]),Y[t].reshape(1,-1))
        #if (t+1) % t_0 == 0:
        if X[t+1,3] != X[t,3]:
            tx2.set_visible(True)
            print "---TRAINING--- (end of day ",(X[t,3]*7),")"
            h.fit(Z[0:t-1],Y[1:t])
            g.fit(Z[0:t-10],Y[10:t])     # <--- train on a significant chunk at a time (sklearn's tree not incremental)
            pause(1)
            tx2.set_visible(False)

        ax1.set_title("Device %d. %s. %s" % (DEV_ID,days[int(X[t,3]*7)-1], t2hr(X[t,2])))
        #images.append([copy.deepcopy(m),copy.deepcopy(mp)])
        pause(0.001)
        #writer.grab_frame()

ioff()
