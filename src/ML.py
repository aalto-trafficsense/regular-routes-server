from numpy import *
from transforms import *
import copy
import LP
import BR

class ML(LP.LP) :
    '''
    Meta-Label Classifier
    --------------------------
    Essentially 'RAkELd'. When I add pruning option (inherit PS.PS instead of LP), it will be pretty much the algorithm we used in the ICDM paper.
    I can also wrap it in a subset ensemble for even more scalability.
    '''

    k = 3

    def train(self, X, Y):
        Yy,self.reverse = transform_BR2ML(Y,self.k)
        N_, L_ = Yy.shape
        self.h = BR.BR(L_,copy.deepcopy(self.h))
        self.h.train(X,Yy)
        #self.h.fit(X, y)

    def predict(self, X):
        '''
            return predictions for X
        '''
        Yy = self.h.predict(X)
        N,D = X.shape
        Y = transform_ML2BR(Yy,self.reverse,self.L,self.k)
        return Y

def demo():
    from tools import make_XOR_dataset

    X,Y = make_XOR_dataset()
    N,L = Y.shape

    from sklearn import linear_model
    h = linear_model.LogisticRegression()
    h = linear_model.SGDClassifier(n_iter=100)
    ml = ML(L, h)
    ml.train(X, Y)
    # test it
    print ml.predict(X)
    print "vs"
    print Y

if __name__ == '__main__':
    demo()

