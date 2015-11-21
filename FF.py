from numpy import *
set_printoptions(precision=2)

POS = [0,1]
LAT = 0
LON = 1
TOD = 2
DOW = 3

BIAS = 0
XSTAT = [1,2,3,4]
ZPOS = [1,2]
ANGLE = 5 #DIREC = [5, 6]
SPEED = 6
SPEE2 = 7
ACCEL = 8
VARIA = 9
WKEND = 10
N_END = 11

def norm(x):
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
        ##### TODO: WILL ALSO WANT TO CHECK TOD COMPARED TO THE LAST TOD
        ##### IF THERE IS A BIG GAP, THEN WE ARE FILTERING .. AND ASSUME PREVIOUS POINTS WERE STATIONARY (MAY WORK ANYWAY?)
        #print x, "->",

        z = zeros(self.N_h)    # nodes
        z[BIAS] = 1.         # output bias node

        _z = self.Z[0]

        if abs(x[TOD] - _z[XSTAT][TOD]) > 0.034722:
            print " THERE WAS A GAP OF ",abs(x[TOD] - _z[XSTAT][TOD])*24.," HRS SINCE THE LAST MEASUREMENT, IGNORE THIS PREVIOUS Z"
            print "_x", _z[XSTAT]
            print " x", x
            _z[XSTAT] = x[:]
            _z[XSTAT][TOD] = x[TOD] - 0.034722

        z[XSTAT] = x[:]                                               # x
        z_DIR = x[POS] - _z[ZPOS]                                     # direction vector
        z[ANGLE] = (arctan(z_DIR[0]/z_DIR[1]) + pi) / (2.*pi)         # angle of direction vector (normalized)
        z[SPEED] = norm(z_DIR)                                        # speed of direction vector
        z[SPEE2] = _z[SPEED]                                          # speed
        z[ACCEL] = (z[SPEED] - _z[SPEED])**2                          # accel
        z[VARIA] = var(array([z[SPEED],z[SPEE2]])) * 10.              # variance
        z[WKEND] = (x[DOW] == 0. or x[DOW] == 1.)                     # is saturday or sunday (on normalized data)
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
