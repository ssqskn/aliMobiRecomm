import numpy as np
from sklearn import ensemble

def fitRForest(train, target, N_rfs, params):
    ## unpack parameters
    ntree, maxfea, leafsize = params[0] 
    weight = np.ones(len(target))
    
    rfs = [ensemble.RandomForestClassifier(n_estimators=ntree, max_features=maxfea,
        min_samples_leaf=leafsize,random_state=i) for i in range(N_rfs)]
    
    rfs_trained = [i.fit(train,target,weight) for i in rfs]
    
    return rfs_trained