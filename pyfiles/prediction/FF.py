from numpy import *

POS = [0,1]
LAT = 0
LON = 1
TOD = 2
DOW = 3

BIAS = 0
XSTAT = [1,2,3,4]
ZPOS = [1,2]
N_v_S = 5
E_v_W = 6
N_o_S = 7 
E_o_W = 8 
                        #ANGLE = 5
                        #ANG_O = 6
SPEED = 9
SPE_O = 10
ACCEL = 11
                        #VARIA = 12
WKEND = 12
N_END = 13
AM_PM = 13

def magnitude(x):
    return sqrt(dot(x,x))

class FF():

    ''' 
        Recurrent Basis/Transformation Function
        ---------------------------------------
        Turn x into \phi in a recurrent manner.
    '''

    Z = None
    def __init__(self, N_i, buf=10):
        '''
            Z:
                0-4     copy of x
                0       bias
                1       direction angle (from x[t-1])
                2       speed
                3       speed of x[t-1]
                4       acceleration (speed of last reading - speed)
                5       variance (of x[t-n,...,t].speed)
                6       time since last stop
                7       weekend vs weekday
        '''

        self.N_i = N_i          # inputs
        self.N_h = N_END      # outputs
        self.Z = zeros((buf,self.N_h)) # buffer
        self.b = 0 # buffer entry position

    def phi(self,x):

        z = zeros(self.N_h)    # nodes
        z[BIAS] = 1.           # output bias node (not needed for decision trees, but anyway..)

        _z = self.Z[0]

        FIVE_MINUTES = 5./60.
        if abs(x[TOD] - _z[XSTAT][TOD]) > FIVE_MINUTES:
            print("[FF.py] WARNING: There was a gap of ",abs(x[TOD] - _z[XSTAT][TOD])," hours since the previous measurement. (This is the first one, the device has been turned off, or part of the trace is missing).")
            ####
            #### TODO: CHECK DISTANCE FROM LAST POINT, AND THEN AVERAGE SENSIBLY ACROSS TIME TO GET AVERAGE SPEED, ETC.
            ####
            _z[XSTAT] = x[:]
            _z[XSTAT][TOD] = x[TOD] - FIVE_MINUTES

        z[XSTAT] = x[:]                                               # x
        z_DIR = x[POS] - _z[ZPOS]                                     # direction vector
        z_DIR_n = z_DIR/(sum(abs(z_DIR))+0.0000001)
        z[N_o_S] = _z[N_v_S]
        z[E_o_W] = _z[E_v_W]
        z[N_v_S] = z_DIR_n[0]
        z[E_v_W] = z_DIR_n[1]
        #z[ANGLE] = (arctan(z_DIR[0]/z_DIR[1]) + pi) / (2.*pi)         # @TODO break into N/S and E/W components
        #z[ANG_O] = _z[ANGLE]                                          # angle of direction vector (normalized)
        z[SPEED] = magnitude(z_DIR)                                    # speed of direction vector
        z[SPE_O] = _z[SPEED]                                          # speed   @TODO scale+cap sensibly
        z[ACCEL] = (z[SPEED] - _z[SPEED])**2                          # accel   @TODO scale+cap sensibly
        #z[VARIA] = var(array([z[SPEED],z[SPE_O]])) * 10.              # variance
        z[WKEND] = (x[DOW] == 0. or x[DOW] == 6.)                     # is saturday or sunday (on normalized data)
        #z[AM_PM] = (x[TOD] > 0.5)                                     # is morning or afternoon (on normalized data)
        z = nan_to_num(z)
        #z[end+3:end+4] = self.Z[t-1,SPEED]                     # speed
        #z[end+8] = time since last stop

        #### NOTE: FOR NOW, JUST STORE THE PREVIOUS Z (NOT A WHOLE HISTORY -- CAN DO THAT LATER)
        #self.b = (self.b + 1) % self.Z.shape[0]
        self.Z[0] = z[:]
        #print z, "(", x
        return z

    def reset(self):
        self.z = self.z * 0.
        self.z[0] = 1.
