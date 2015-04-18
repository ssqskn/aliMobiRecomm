import numpy as np
import multiprocessing
from utils import dump_pickle,_load_pickle
from sklearn import ensemble        

def mulTrain(rf):
    train = _load_pickle("debug/train.pkl")
    target = _load_pickle("debug/target.pkl")
    weight = _load_pickle("debug/weight.pkl")
    return rf.fit(train,target,weight)


def fitRForest(train, target, N_rfs, params):
    ## unpack parameters
    ntree, maxfea, leafsize = params[0] 
    weight = np.ones(len(target))
    dump_pickle(train,"debug/train.pkl")
    dump_pickle(target,"debug/target.pkl")
    dump_pickle(weight,"debug/weight.pkl")

    rfs = [ensemble.RandomForestClassifier(n_estimators=ntree, max_features=maxfea,
        min_samples_leaf=leafsize,random_state=i) for i in range(N_rfs)]
    
    rfs_trained = []
    
    pool = multiprocessing.Pool(3)
    for i,rf_trained in enumerate(pool.imap(mulTrain,rfs)):
        rfs_trained.append(rf_trained)
    pool.terminate()
    #rfs_trained = [i.fit(train,target,weight) for i in rfs]
    
    return rfs_trained