#!/usr/bin/python

# Scientific libraries
from numpy import *
set_printoptions(precision=5, suppress=True)

# Other necessary libraries
import copy

##################################################################################
#
# Parameters
#
##################################################################################

DEV_ID = 45
bx = (60.1442, 24.6351, 60.3190, 25.1741)
b = 10              # majic window parameter
thres = 0.001       # relative movement threshold for not-filtering
win_pred = 10       # how far to predict into the future

##################################################################################
#
# Load trace
#
##################################################################################

X = None

if False:

    import psycopg2

    try:
        conn = psycopg2.connect("dbname='regularroutes' user='regularroutes' host='localhost' password='TdwqStUh5ptMLeNl' port=5432")
    except:
        print "I am unable to connect to the database"

    c = conn.cursor()

    print "Extracting trace"
    c.execute('SELECT hour,day_of_week,longitude,latitude FROM averaged_location WHERE device_id = %s', (str(DEV_ID),))
    dat = array(c.fetchall(),dtype={'names':['H', 'DoW', 'lon', 'lat'], 'formats':['i4', 'i4', 'f4','f4']})
    run = column_stack([dat['lon'],dat['lat']])
    X = column_stack([dat['lon'],dat['lat'],dat['H'],dat['DoW']])
    savetxt('./dat/'+str(DEV_ID)+'_stream_X.csv', X, delimiter=',')

X = genfromtxt('./dat/'+str(DEV_ID)+'_stream_X.csv', skip_header=0, delimiter=',')

##################################################################################
#
# Load clusters and Snap to Them
#
##################################################################################

if False:

    import psycopg2

    try:
        conn = psycopg2.connect("dbname='regularroutes' user='regularroutes' host='localhost' password='TdwqStUh5ptMLeNl' port=5432")
    except:
        print "I am unable to connect to the database"

    c = conn.cursor()

    print "Extracting waypoints"
    c.execute('SELECT latitude, longitude FROM cluster_centers WHERE device_id = %s', (str(DEV_ID),))
    rows = c.fetchall()
    nodes = array(rows)
    print nodes.shape

    print "Snapping past to these ", len(nodes)," waypoints"
    import sys
    sys.path.append("src/")
    from utils import snap
    Y = snap(X[:,0:2],nodes)

    savetxt('./dat/'+str(DEV_ID)+'_stream_Y.csv', Y, delimiter=',')

Y = genfromtxt('./dat/'+str(DEV_ID)+'_stream_Y.csv', skip_header=0, delimiter=',')

##################################################################################
#
# Transforming data
#
##################################################################################

def center(x,min_x,max_x):
    return (x - min_x) / (max_x - min_x)

def uncenter(x,min_x,max_x):
    return x * ( max_x - min_x ) + min_x

mins = zeros(4)
maxs = zeros(4)

for j in range(4):
    mins[j] = min(X[:,j])
    maxs[j] = max(X[:,j])

# Center the data
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
i = b
for t in range(b,T):
    ee = X[t-b:t-1,0:2] - X[t,0:2]
    d = sqrt(max(ee[:,0])**2 + max(ee[:,1])**2)
    if d > thres:
        X_[i,:] = X[t,:]
        i = i + 1
X = X_[0:i,:]
print "... from ", T, "examples to", i

T,D = X.shape

# Pass through an ESN filter
print "Pass thorugh ESN filter"
import sys
from sklearn.kernel_approximation import RBFSampler
rbf = RBFSampler(gamma=1, random_state=1)
sys.path.append("/home/jesse/Dropbox/Projects/ALife/mad")
from RTF import RTF
from MOP import linear
H=100
rtf = RTF(D,H,f=tanh,density=0.1)
Z = zeros((T,H))
for t in range(T):
    Z[t] = rtf.phi(X[t])
print "... turned ", X.shape, "into", Z.shape

##################################################################################
#
# Machine Learning and Plotting
#
##################################################################################

# Prepare for plotting
import matplotlib
matplotlib.use('Qt4Agg')
from matplotlib.pyplot import *
ion()

print "Get Map"
import smopy
#map = smopy.Map(bx,z=12)
import joblib
#joblib.dump(map,  './dat/smap.dat')
map = joblib.load('./dat/smap.dat' )

'''
ax2 = map.show_mpl(figsize=(8, 6))
map.show_ipython()
uni = [60.186228, 24.830285]
x, y = map.to_pixels(uni[0], uni[1])                # <--- test
tt, = ax2.plot(x, y, 'or-', markersize=5);
#plot(x, y, 'or', markersize=10);
CC = array(map.to_pixels(uncenter(X[:,0],mins[0],maxs[0]),uncenter(X[:,1],mins[1],maxs[1]))).T
print CC
print CC.shape
pause(2)

tt.set_xdata(CC[:,0])
tt.set_ydata(CC[:,1])
print CC
show()
'''

print "Setup"

# Multi-output Regressior
from MOR import MOR
from sklearn.linear_model import SGDRegressor
from sklearn.neighbors import KNeighborsRegressor
h = MOR(2,SGDRegressor())
#h.partial_fit(Z[0:b],X[1:b+1,0:2])
h.fit(Z[0:T-1],X[1:T,0:2])

# With init. batch of b, until T.
window = zeros((b,2))
history = zeros((T))

fig = figure()
from matplotlib import gridspec
gs = gridspec.GridSpec(2, 1, height_ratios=[1, 6]) 
ax0 = fig.add_subplot(gs[0])
l, = ax0.plot(0,0,'k-',markersize=10,linewidth=1)
grid(True)
ax0.set_title("error / "+str(len(X)))

ax1 = fig.add_subplot(gs[1])
ax1.set_title("map")
img = imread('Helsinki.png') #
ax1.imshow(img)
ax1.set_xlim([0,img.shape[1]]) #
ax1.set_ylim([img.shape[0],0]) #
#ax1.set_xlim([-0.1,+1.1])
#ax1.set_ylim([-0.1,+1.1])
mp, = ax1.plot(0,0,'ro-',markersize=2,linewidth=3)
m, = ax1.plot(0,0,'bo-',markersize=1,linewidth=3)
gs.tight_layout(fig)


show()

def tick(xc,xp):
    return ""

print "Go"
for t in range(b,T-1):

    # Plot the trace
    t0 = max(0,t-b)
    MM = array(map.to_pixels(uncenter(X[t0:t,0],mins[0],maxs[0]),uncenter(X[t0:t,1],mins[1],maxs[1]))).T
    #px, py = scaled2pixels(X[t0:t,0], X[t0:t,1], img.shape[1], img.shape[0], bx)
    #print "PXY\n", px, py, MM.shape
    m.set_xdata(MM[:,0])
    m.set_ydata(MM[:,1])
    #m.set_xdata(X[max(t-10000,t-b):t,0]*img.shape[1])
    #m.set_ydata(X[max(t-10000,t-b):t,1]*img.shape[0])

    # Plot the error window
    b_ = t % b
    yp = h.predict(Z[t-1].reshape(1,-1))
    window[b_,0] = sqrt((X[t,0] - yp[0,0])**2)
    window[b_,1] = sqrt((X[t,1] - yp[0,1])**2)
    history[t-b] = mean(mean(window,axis=0))
    print "AVG", history[t-b], X[t], " LEARN:", array([Z[t]]), "->", X[t+1,0:2]
    h.partial_fit(array([Z[t]]),X[t+1,0:2].reshape(1,-1))
    l.set_data(range(0,t-b),history[0:t-b])
    ax0.set_xlim([0,t-b])
    ax0.set_ylim([0,max(history)])

    # Predict into the future
    xp = copy.deepcopy(X[t])
    XP = zeros((win_pred,2))
    rtf2 = copy.deepcopy(rtf)
    for i in range(0,win_pred):
        zp = rtf2.phi(xp)
        XP[i,:] = h.predict(zp.reshape(1,-1))       # predict coordinates
        xp[0:2] = XP[i,:]
        print " PRED", xp
        #xp = tick(xc,xp)    # plug in a fake time-increment with the predicted coordinates, and proceed ...
    MMM = array(map.to_pixels(uncenter(XP[:,0],mins[0],maxs[0]),uncenter(XP[:,1],mins[1],maxs[1]))).T
    mp.set_xdata(MMM[:,0])
    mp.set_ydata(MMM[:,1])

    pause(0.001)

ioff()
