#!/usr/bin/python

# Scientific libraries
from numpy import *
random.seed(0)
set_printoptions(precision=2, suppress=True)

# Prepare for plotting
import matplotlib
matplotlib.use('Qt4Agg')
from matplotlib.pyplot import *

import sys
import os.path

##################################################################################
#
# Parameters
#
##################################################################################

DEV_ID = 45
use_test_server = False

if len(sys.argv) > 2:
    use_test_server = (sys.argv[2] == 'test')
    DEV_ID = int(sys.argv[1])
if len(sys.argv) > 1:
    DEV_ID = int(sys.argv[1])
else:
    print("specify device id, and 'test' if you want to use the test server")
    print("e.g.: python run_demo.py 45 test")
    exit(1)

T_p = 3            # how far to predict (route) into the future
T_h = 5            # number of steps (minutes) to display of up-till-now trajectory
T_g = 5            # how far ahead to predict (destination) in the future
T_b = 20           # number of steps (minutes) to consider a break in trasport
min_metres = 30    # relative movement threshold for not-filtering

from .FF import FF

##################################################################################
#
# Load Map
#
##################################################################################
#bx = (60.1442, 24.6351, 60.3190, 25.1741)   # Helsinki 1
bx = (60.2795, 24.6307, 60.1543, 25.0000)   # Helsinki 1
FILE_M = './dat/Helsinki_45.dat' #'+str(DEV_ID)+'_
FILE_I = './dat/Helsinki_45.png' #'+str(DEV_ID)+'_

if DEV_ID > 90:
    bx = (60.2905, 24.6307, 60.1543, 25.0000)   # Helsinki 1
    FILE_M = './dat/Helsinki_98.dat' #'+str(DEV_ID)+'_
    FILE_I = './dat/Helsinki_98.png' #'+str(DEV_ID)+'_

#bx = (min(nodes[:,0]), min(nodes[:,1]), max(nodes[:,0]), max(nodes[:,1]))
print("Get Map. Checking for file:", FILE_I, "...")
import smopy
import joblib
if not os.path.isfile(FILE_M):
    print("Creating Map...")
    map = smopy.Map(bx,z=12)
    print("Saving Map...")
    joblib.dump(map,  FILE_M)
    print("Saving Map Image ...")
    map.save_png(FILE_I)
map = joblib.load(FILE_M)

##################################################################################
#
# Important Functions
#
##################################################################################

def t2hr(tt):
    hh = int(tt)
    mm = int((tt - hh) * 60)
    return "%02dh%02d" % (hh,mm)

days = "Sun Mon Tue Wed Thu Fri Sat".split(" ")
def d2day(tt):
    ''' tt is between 0 and 6 inclusive '''
    return days[int(tt)] #days[int(tt*6)]

def tick(z, minutes):
    return z + minutes/24./60.

def get_end_of_first_day(xxxx):
    '''
    @TODO check
    '''
    d = xxxx[0,3]
    for i in range(1,len(xxxx)-T_h):
        if xxxx[i,3] != d:
            return i
    return len(xxxx)-T_h

def conf2line(c):
    return clip((c * 10.)-3,0,7)

def conf2style(c):
    return '-'
    if c < 0.3:
        return ':'
    if c < 0.5:
        return '--'
    else:
        return '-'

def conf2mark(c):
    return c * 5.


from .db_utils import get_conn, get_cursor

##################################################################################
#
# Load trace from database
#
##################################################################################

XXX = None

FILE_X = './dat/'+str(DEV_ID)+'_stream_X.csv'

if not os.path.isfile(FILE_X):

    conn = get_conn(use_test_server) 
    c = conn.cursor()

    if not use_test_server:
        print("Building averaged_location table with new data.")
        sql = open('../..//sql/make_average_table.sql', 'r').read()
        c.execute(sql)
        conn.commit()

    print("Extracting trace")
    c.execute('SELECT hour,minute,day_of_week,longitude,latitude FROM averaged_location WHERE device_id = %s', (str(DEV_ID),))
    dat = array(c.fetchall(),dtype={'names':['H', 'M', 'DoW', 'lon', 'lat'], 'formats':['f4', 'f4', 'i4', 'f4','f4']})
    XXX = column_stack([dat['lon'],dat['lat'],dat['H']+(dat['M']/60.),dat['DoW']])
    savetxt(FILE_X, XXX, delimiter=',')

XXX = genfromtxt(FILE_X, skip_header=0, delimiter=',')
T,D = XXX.shape


from .pred_utils import do_cluster, do_snapping, do_movement_filtering, do_feature_filtering 

def do_process_segment(X):
    '''
        Process X as if it was the full segment.
        ----------------------------------------
        INPUT: X (raw data up)
        OUTPUT: filtered X, and fancy new features Z.

        Save some files to disk so we don't need to fetch from the database everytime (allows for offline work on the prediction).
    '''

    ##################################################################################
    #
    # Filtering the data
    #
    ##################################################################################

    FILE_S = './dat/'+str(DEV_ID)+'_stream_S.csv'

    X_small = None
    if os.path.isfile(FILE_S):
        X_small = genfromtxt(FILE_S, skip_header=0, delimiter=',')
    else:
        X_small = do_movement_filtering(X,min_metres)
        savetxt(FILE_S, X_small, delimiter=',')

    ############################
    # Feature Filter (ESN-style)
    ############################

    FILE_Z = './dat/'+str(DEV_ID)+'_stream_Z.csv'

    Z = None
    if os.path.isfile(FILE_Z):
        Z = genfromtxt(FILE_Z, skip_header=0, delimiter=',')
    else:
        Z = do_feature_filtering(X_small)
        savetxt(FILE_Z, Z, delimiter=',')


    return X_small,Z




##################################################################################
#
# Machine Learning and Plotting
#
##################################################################################

############################
# Setup
############################
print("Setup")
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
#h = RandomForestClassifier(n_estimators=100)

############################
# Initial window training
############################

X,Z = do_process_segment(copy(XXX)) # everything
T,D = X.shape
Y = zeros(T) 
print("Processed everything from ", XXX.shape, "to", X.shape, Z.shape)
t_0 = get_end_of_first_day(X)

#g = tree.DecisionTreeClassifier()
#g30 = tree.DecisionTreeClassifier()
g = RandomForestClassifier(n_estimators=100)
g30 = RandomForestClassifier(n_estimators=100)

############################
# Setup Plot
############################
ion()
fig = figure(figsize=(16.0, 10.0))
from matplotlib import gridspec
gs = gridspec.GridSpec(1, 1)
ax1 = fig.add_subplot(gs[0]) # was: [1]
ax1.set_title("Map, device ID "+str(DEV_ID),fontsize=40)
img = imread(FILE_I) #
ax1.imshow(img)
ax1.set_xlim([0,img.shape[1]]) #
ax1.set_ylim([img.shape[0],0]) #
nodes = None
node_pxs = None
l_0, = ax1.plot(0,0,'ro',markersize=6,linewidth=3,label="current position")
l_, = ax1.plot(0,0,'r-',markersize=1,linewidth=4,label="prev. "+str(T_h)+"-min trajectory")
#mq, = ax1.plot(0,0,'co-',markersize=2,linewidth=5,label="5-min route prediction")
ax1.set_yticklabels([])
ax1.set_xticklabels([])
ax1.grid(False)
msg = "Hey device %d, if you stop at shop X\n,in the next 30 minutes you get\n 10%% discount. Discount code: 123" % (DEV_ID)
tx1 = ax1.text(img.shape[1]/2, img.shape[0]/2, msg, style='italic', fontsize=15, bbox={'facecolor':'red', 'alpha':0.5, 'pad':10})
tx1.set_visible(False)
tx2 = ax1.text(img.shape[1]/2, img.shape[0]/2, "UPDATING MODEL", fontsize=25, bbox={'facecolor':'blue', 'pad':10})
tx2.set_visible(False)
tx3 = ax1.text(5,25, "pred.conf. X%", fontsize=15, color='purple', bbox={'facecolor':'white', 'pad':2})
tx3.set_visible(False)
gs.tight_layout(fig,h_pad=0.1)
legend()

#figManager = get_current_fig_manager()
#figManager.window.showMaximized()
show()

print("Go")
day_count = 1

import matplotlib.animation as animation
FFMpegWriter = animation.writers['ffmpeg']
metadata = dict(title='Simulation Device '+str(DEV_ID), artist='Jesse', comment='Demo')
writer = FFMpegWriter(fps=15, bitrate=6000, metadata=metadata)
with writer.saving(fig, "dat/Regular_Routes_Dev_"+str(DEV_ID)+".mp4", 100):
#if True:
    for t in range(1,t_0):
        XX = map.to_pixels(X[max(0,t-T_h):t,0:2])
        l_0.set_xdata(XX[-1,0])
        l_0.set_ydata(XX[-1,1])
        l_.set_xdata(XX[:,0])
        l_.set_ydata(XX[:,1])
        ax1.set_title("Device %d. Day %d, %s. %s" % (DEV_ID,day_count,d2day(X[t,3]), t2hr(X[t,2])),fontsize=25)
        pause(0.001)
        writer.grab_frame()

    tx2.set_visible(True)
    print("---INITIAL BUILD--- (end of day ",(X[t,3]),", i.e., ", d2day(X[t,3]),")")
    pause(0.1)
    nodes = do_cluster(X[0:t_0])
    Y[0:t_0] = do_snapping(X[0:t_0],nodes)
    node_pxs = map.to_pixels(nodes)
    h.fit(Z[0:t_0-1],Y[1:t_0])       # <--- train on a significant chunk at a time (sklearn's tree not incremental)
    g.fit(Z[0:t_0-T_g],Y[T_g:t_0])     # <--- train on a significant chunk at a time (sklearn's tree not incremental)
    g30.fit(Z[0:t_0-T_b],Y[T_b:t_0])     # <--- train on a significant chunk at a time (sklearn's tree not incremental)
    for iii in range(30):
        writer.grab_frame()
    tx2.set_visible(False)
    day_count = 2

    l_n, = ax1.plot(node_pxs[:,0],node_pxs[:,1],'bo',markersize=8,label="personal nodes")
    l_5, = ax1.plot(0,0,'mo-',markersize=2,linewidth=3,label=str(T_p)+"-min route prediction")
    l_10, = ax1.plot(0,0,'mo',markersize=15,fillstyle='none',label=str(T_g)+"-min destination pred.")
    l_30, = ax1.plot(0,0,'ms',markersize=16,fillstyle='none',label=str(T_b)+"-min destination pred.")
    legend()
    tx3.set_visible(True)

    for t in range(t_0,T-1):
        print("[%d] (%3.2f %%) " % (t, t*100./T), Z[t,1:5], X[t,0:4])

        #######################
        # Plot the trace
        #######################
        XX = map.to_pixels(X[max(0,t-T_h):t,0:2])
        l_0.set_xdata(XX[-1,0])
        l_0.set_ydata(XX[-1,1])
        l_.set_xdata(XX[:,0])
        l_.set_ydata(XX[:,1])

        #######################
        # Predict into the future, P(y[t+1]|X[t])
        #######################
        yp_g = g.predict(Z[t].reshape(1,-1)).astype(int)[0]
        py_g = max(g.predict_proba(Z[t].reshape(1,-1))[0])
        gnp = node_pxs[yp_g]
        l_10.set_markeredgewidth(conf2mark(py_g))
        l_10.set_data(gnp)

        yp_g30 = g30.predict(Z[t].reshape(1,-1)).astype(int)[0]    # predict node
        py_g30 = max(g30.predict_proba(Z[t].reshape(1,-1))[0])            # among these confidences
        gnp30 = node_pxs[yp_g30]

        l_30.set_markeredgewidth(conf2mark(py_g30))
        l_30.set_data(gnp30)


        tx3.set_text("Prediction Confidence: %3.2f (10 min), %3.2f (30 min)" % (py_g, py_g30))

        #######################
        # Predict a full trace
        #######################
        YP = zeros((T_p,2))         # we will predict T_p steps ahead
        YP[0] = XX[-1,:]            # end of present trace
        rtf2 = FF(D,T_h)             # make a new filter
        rtf2.Z[0] = Z[t][:]         # prime it
        xp = copy(X[t+1])

        p_ypp = zeros(T_p)
        for i in range(1,T_p):
            zp = rtf2.phi(xp[:])
            ypp = h.predict(zp.reshape(1,-1)).astype(int)[0]       # predict coordinates
            p_ypp[i] = h.predict_proba(Z[t].reshape(1,-1))[0][ypp] # with this confidence
            YP[i] = node_pxs[ypp]                                  # MIG-coords
            cpp = nodes[ypp]                                       # ML-coords
            xp[0:2] = cpp
            xp[2] = tick(xp[2],1.) 

        l_5.set_linewidth(conf2line(p_ypp[1]))
        l_5.set_linestyle(conf2style(p_ypp[1]))
        l_5.set_xdata(YP[:,0])
        l_5.set_ydata(YP[:,1])

        #######################
        # Update classifier
        #######################
        #h.partial_fit(array([Z[t-1]]),X[t+1,0:2].reshape(1,-1))
        #h.partial_fit(array([Z[t-1]]),Y[t].reshape(1,-1))
        #if (t+1) % t_0 == 0:
        if X[t+1,3] != X[t,3]:
            tx2.set_visible(True)
            l_5.set_xdata(0)
            l_5.set_ydata(0)
            l_10.set_xdata(0)
            l_10.set_ydata(0)
            l_30.set_xdata(0)
            l_30.set_ydata(0)
            pause(0.1)
            print("---TRAINING--- (end of day ",(X[t,3]),", i.e., ", d2day(X[t,3]),")")
            print(0,t_0,t, end=' ') 
            for iii in range(30):
                writer.grab_frame()
            tx2.set_text("UPDATING NODES")
            pause(0.1)
            nodes = do_cluster(X[0:t],min(10+day_count*5,50))
            Y[0:t] = do_snapping(X[0:t],nodes)
            node_pxs = map.to_pixels(nodes)
            l_n.set_xdata(node_pxs[:,0])
            l_n.set_ydata(node_pxs[:,1])
            h.fit(Z[0:t-1],Y[1:t])
            g.fit(Z[0:t-T_g],Y[T_g:t])     # <--- train on a significant chunk at a time (sklearn's tree not incremental)
            g30.fit(Z[0:t-T_b],Y[T_b:t])     # <--- train on a significant chunk at a time (sklearn's tree not incremental)
            pause(0.1)
            tx2.set_text("UPDATING MODELS")
            tx2.set_visible(False)
            day_count = day_count + 1

        #print X[t,2], Z[t,3]
        ax1.set_title("Device %d. Day %d (%d nodes), %s. %s" % (DEV_ID,day_count,len(nodes),d2day(X[t,3]), t2hr(X[t,2])),fontsize=25)
        pause(0.001)
        writer.grab_frame()

ioff()
