from numpy import *
set_printoptions(precision=4)

LAT = 0
LON = 1
TOD = 2
DOY = 3

XSTAT = [1,2,3,4]
BIAS = 0
ZPOS = [1,2]
DIREC = 5
SPEED = 6
ACCEL = 7
VARIA = 8
WKEND = 9

class FF():

    ''' 
        Recurrent Basis/Transformation Function
        ---------------------------------------
        Turn x into \phi in a recurrent manner.
    '''

    W_hh = None
    W_ih = None
    z = None

    def __init__(self, N_i, N_h, buf=10):
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
        self.N_h = N_i + 7      # outputs
        self.Z = zeros((buf,N_h)) # buffer
        self.b = 0 # buffer entry position

    def phi(self,x):
        ##### TODO: WILL ALSO WANT TO CHECK TOD COMPARED TO THE LAST TOD
        ##### IF THERE IS A BIG GAP, THEN WE ARE FILTERING .. AND ASSUME PREVIOUS POINTS WERE STATIONARY (MAY WORK ANYWAY?)

        z = zeros(N_h)    # nodes
        z[BIAS] = 1.         # output bias node

        print x,"-->",
        _z = self.Z[0]

        z[1:self.N_i+1] = x[:]                                        # x
        z[DIREC] = x[POS] - _z[ZPOS]                                  # direction vector
        z[SPEED] = norm(z[end+0:end+2])                               # speed
        z[ACCEL] = z[SPEED] - _z[SPEED]                               # accel
        z[VARIA] = var(array([self.z[SPEED],_z[SPEED]))               # variance
        z[WKEND] = (self.x[DOW] == 0. or self.x[DOW] == 1.)           # is saturday or sunday (on normalized data)
        #z[end+3:end+4] = self.Z[t-1,SPEED]                     # speed
        #z[end+8] = time since last stop
        print z

        #### NOTE: FOR NOW, JUST STORE THE PREVIOUS Z (NOT A WHOLE HISTORY -- CAN DO THAT LATER)
        #self.b = (self.b + 1) % self.Z.shape[0]
        self.Z[0] = z[:]
        return z

    def reset(self):
        self.z = self.z * 0.
        self.z[0] = 1.
