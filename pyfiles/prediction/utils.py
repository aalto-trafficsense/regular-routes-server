from numpy import *

############# OBSERVATION FN #########################

def get_observation(nodes, s):
    '''
        An observation on state s is the actual position + some Gaussian noise
    '''
    return nodes[s,:] + random.randn(1,2)

###################################################
# 
#   SIMULATED DATA
# 
###################################################

def gen_simulated_data(nodes, P=8, W=8):

    '''
    GENERATE A NUMBER OF PATHS BETWEEN NODES
    ===========================================
        P:  number of distinct journeys
        W:  length of each journey
        --------------------------------
        L:  the number of nodes
    '''
    L,D = nodes.shape

    ############# GENERATE PATHS #########################

    states = range(1,L-1)
    X = zeros((P,W))
    X[:,0] = 0
    X[:,-1] = L-1

    for i in range(P):
        random.shuffle(states)
        X[i,1:W-1] = states[0:W-2]

    NN = 100         # number of instances

    Z = zeros((NN,2))
    for i in range(NN):

        # 1. GENERATE (RANDOM) WEATHER
        weather_ = random.rand(1)*10
        # 2. GENERATE (PICK ONE) START TIME
        h = random.choice([8,17])
        # 3. GENERATE (SELECT) TRAJECTORY
        traj = random.choice(range(P))

        x = None
        if h == 17:
            x = array(X[traj,::-1],dtype=int)
        else:
            x = array(X[traj,:],dtype=int)
        Z[i,:] = array([weather_,h])

        # 4.    GENERATE OBSERVATIONS (For each timestep in trajectory):
        y = zeros((W,2))
        for c in range(W):
            y[c] = get_observation(nodes,X[traj,c])

        # 5.    PRINT H,W,N,X,Y
        #print array2csv(x)+","+array2csv(weather_)+","+str(h)+","+array2csv(y.flatten())

    return X,y


def snap(run,stops):
    '''
        SNAP
        =======
        snap points in 'run' to points in 'stops', return the indices.
    '''
    T = run.shape[0]
    y = zeros(T,dtype=int)
    for t in range(T):
        #print t, "/", T
        p = run[t,:]
        dvec = sqrt((stops[:,0]-p[0])**2 + (stops[:,1]-p[1])**2)
        i = argmin(dvec)
        y[t] = i 
    return y

def shufl(X,Y):
    indices = range(X.shape[0])
    random.shuffle(indices)
    return X[indices,:], Y[indices,:]

def stack_stream(X,y,win_past=5,win_futr=5):
    '''
        Stack
        -----
        Stack the stream X,y into multi-label dataset X,Y where 
            each Y[i,:] contains the points at y[t],...,y[t+win_fut]
            each X[i,:] contains points at X[t] and also y[t-win_past:t-1]
    '''

    N,D = X.shape

    Y_ = zeros((N,win_futr))
    X_ = zeros((N,D+win_past))

    for i in range(win_past,N-win_futr):
        X_[i,:D] = X[i,:]
        X_[i,D:] = y[i-win_past:i]
        Y_[i,:] = y[i:i+win_futr]

    X_ = X_[win_past:N-win_futr,:]
    Y_ = Y_[win_past:N-win_futr,:]

    return X_,Y_

#X = random.rand(10,2)
#y = random.rand(10)
#print X,y
#XX,YY = stack(X,y,3,3)
#print XX
#print YY

def filter_stream(X,y,n):
    '''
        filter out all instances, where y[t] is the same for n timepoints in a row.
    '''

    y_ = zeros(y.shape)
    X_ = zeros(X.shape)

    y_[0:n] = y[0:n]
    X_[0:n,:] = X[0:n,:]

    i = 0
    for t in range(n,len(y)):
        skip=True
        for n_ in range(1,n):
            if y[t] != y[t-n_]:
                skip=False
                break
        if not skip:
            i = i + 1
            y_[i] = y[t]
            X_[i,:] = X[t,:]

    return X_[0:i,:], y_[0:i]

