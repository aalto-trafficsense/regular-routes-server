#!/usr/bin/python

# Scientific Libraries
from numpy import *
set_printoptions(precision=5, suppress=True)



import copy
#from run_prediction import predict
#train(45,5,5,mod="RNN",lim=lim)


##################################################################################
#
# Prelim
#
##################################################################################

DEV_ID = 45
#from run_clustering import cluster
#cluster(45)


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

else:
    X = genfromtxt('./dat/'+str(DEV_ID)+'_stream_X.csv', skip_header=0, delimiter=',')

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
#X[:,:] = X[:,:] - mean(X[:,:],axis=0)
print mins, maxs
for j in range(4):
    X[:,j] = center(X[:,j],mins[j],maxs[j])
    #X[:,j] = (X[:,j] - min(X[:,j]))
    #X[:,j] = (X[:,j] / max(X[:,j]))

bx = (60.1442, 24.6351, 60.3190, 25.1741)

def scaled2pixels(cx,cy,lx,ly,bx,ucen=True):
    '''
        cx = 0.1
        lx = 1792
        mins = 60.011
        bx = 60.1442 
    '''
    #===================
    #[ 0.0648  0.1546] [ 0.0664  0.1598]
    #1792 1280
    #PXY [-615917 -614996] [-58345 -58123]
    #AVG 0.112344349063 [ 0.1205  0.3392  0.4348  0.1667]

    #print "==================="
    #print cx, cy
    if ucen:
        cx = uncenter(cx,mins[0],maxs[0])
        cy = uncenter(cy,mins[1],maxs[1])
    #print cx, cy
    #print lx, ly
    # from 0.1 to 34.33
    px = center(cx,bx[0],bx[2]) 
    py = center(cy,bx[1],bx[3]) 
    return px * lx, (-py*ly + img.shape[0] )

#tx = array([ 0.0548, 0.0492, 0.0435, 0.1667])
#ty = array([ 0.0548, 0.0492, 0.0435, 0.1667])
#print scaled2pixels(tx,ty,1792,1280)


print min(X[:,0]), min(X[:,1])
print max(X[:,0]), max(X[:,1])
print mean(X[:,0]), mean(X[:,1])

# Filter out boring examples
b = 10              # majic window parameter
print "Filter"

T,D = X.shape

X_ = zeros(X.shape)
X_[0:b,:] = X[0:b,:]
i = b
for t in range(b,T):
    ee = X[t-b:t-1,0:2] - X[t,0:2]
    d = sqrt(max(ee[:,0])**2 + max(ee[:,1])**2)
    if d > 0.001:
    #if var(X[t-b:t,0:2]) > 0.02:
        X_[i,:] = X[t,:]
        i = i + 1
X = X_[0:i,:]
print "... Reduced from ", T, "to", i

T,D = X.shape

# Pass through an ESN filter
print "Pass thorugh ESN filter"
import sys
sys.path.append("/home/jesse/Dropbox/Projects/ALife/mad")
from RTF import RTF
H=20
rtf = RTF(D,H,f=tanh,density=0.2)
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

#ax2 = fig.add_subplot(133)
# Get Map

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
h = MOR(2)
h.partial_fit(Z[0:b],X[1:b+1,0:2])

# With init. batch of b, until T.
window = zeros((b,2))
history = zeros((T))

fig = figure()
from matplotlib import gridspec
gs = gridspec.GridSpec(1, 2, width_ratios=[1, 4]) 
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
mp, = ax1.plot(0,0,'ro-',markersize=2,linewidth=5)
m, = ax1.plot(0,0,'bo-',markersize=1,linewidth=2)
gs.tight_layout(fig)


#uni = [60.3190, 25.1741] # max (lower right)
#uni = [60.1442, 24.6351] # min (top left)
#uni = [60.186228, 24.830285]
#for j in range(2):
#    uni[j] = center(uni[j],mins[j],maxs[j])
#px,py = scaled2pixels(uni[0], uni[1], img.shape[1], img.shape[0], bx, True)
#print px, py
#mp, = ax1.plot(px,py,'ro-',markersize=10,linewidth=3)
#print "###", uni, px,py, img.shape


show()

#from sklearn import preprocessing
#X = preprocessing.scale(X)
#exit(1)
def tick(xc,xp):
    return ""

print "Go"
# Get going on the first examples
for t in range(1,b):
    yp = h.predict(Z[t-1].reshape(1,-1))
    window[t,0] = sqrt((X[t,0] - yp[0,0])**2)
    window[t,1] = sqrt((X[t,1] - yp[0,1])**2)
    history[t] = mean(mean(window,axis=0))
    h.partial_fit(Z[t].reshape(1,-1),X[t+1,0:2].reshape(1,-1))

# Continue to plot over time
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
    print "AVG", history[t-b], X[t]
    h.partial_fit(array([Z[t]]),X[t+1,0:2].reshape(1,-1))
    l.set_data(range(0,t-b),history[0:t-b])
    ax0.set_xlim([0,t-b])
    ax0.set_ylim([0,max(history)])

    # Predict into the future
    xp = copy.deepcopy(X[t])
    XP = zeros((20,2))
    rtf2 = copy.deepcopy(rtf)
    for i in range(0,20):
        zp = rtf2.phi(xp)
        XP[i,:] = h.predict(zp.reshape(1,-1))       # predict coordinates
        xp[0:2] = XP[i,:]
        #xp = tick(xc,xp)    # plug in a fake time-increment with the predicted coordinates, and proceed ...
    MMM = array(map.to_pixels(uncenter(XP[:,0],mins[0],maxs[0]),uncenter(XP[:,1],mins[1],maxs[1]))).T
    mp.set_xdata(MMM[:,0])
    mp.set_ydata(MMM[:,1])

    pause(0.001)

ioff()


#lim = 1425135902
#print train(45,5,5,mod="EML",lim=lim)
#yp,y = predict(45,5,5,"EML",lim=lim,commit_result=False)
#print yp.shape, y.shape
#for i in range(yp.shape[0]):
#    print y[i], "->", yp[i,:]

#predict(45,5,5,"RNN",lim=lim)
