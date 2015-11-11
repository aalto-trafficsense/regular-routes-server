from numpy import *

from numpy import *
import copy
from sklearn import linear_model

class MOR() :
    ''' multi-output regression '''

    h = None
    L = -1

    def __init__(self, L, h=linear_model.SGDRegressor()):
        self.L = L 
        self.h = [ copy.deepcopy(h) for j in range(self.L)]

    def fit(self, X, Y):
        '''
            Fit model
            ---------
        '''
        # Set base classifiers
        # Train them
        for j in range(self.L):
            self.h[j].fit(X, Y[:,j])

    def partial_fit(self, X, Y):
        '''
            Fit model incrementally
            -----------------------
            (Must call fit first)
        '''
        for j in range(self.L):
            self.h[j].partial_fit(X, Y[:,j])

    def predict(self, X):
        '''
            Predict
            -------
        '''
        N,D = X.shape
        Y = zeros((N,self.L))
        for j in range(self.L):
            Y[:,j] = self.h[j].predict(X)
        return Y

def demo():
    print "test"

if __name__ == '__main__':
    demo()

