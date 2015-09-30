from numpy import *
from sklearn import linear_model
from sklearn.lda import LDA
from transforms import *
import copy

class LP() :

    h = None
    L = -1
    reverse = {}

    def __init__(self, L, h):
        self.L = L
        self.h = copy.deepcopy(h)

    def train(self, X, Y):
        y,self.reverse = transform_BR2MC(Y)
        #print "train with " , len(self.reverse) , " unique classes"
        self.h.fit(X, y)

    def predict(self, X):
        '''
            return predictions for X
        '''
        y = self.h.predict(X)
        N,D = X.shape
        Y = transform_MC2BR(y,self.L,self.reverse)
        return Y
