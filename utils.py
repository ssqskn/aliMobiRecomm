#coding=utf-8
import pickle

##dump pickle
def dump_pickle(dct,PATH):
    with open(PATH, 'wb') as f:
        pickle.dump(dct, f)
        
##reload pickle
def _load_pickle(PATH):
    with open(PATH, 'rb') as f:
        dct = pickle.load(f)
    return dct

def load_pickle():
    userFeatures         = _load_pickle("pickle\\userFeatures.pickle")
    itemFeatures         = _load_pickle("pickle\\itemFeatures.pickle")
    categoryFeatures    = _load_pickle("pickle\\categoryFeatures.pickle")
    userItemFeatures     = _load_pickle("pickle\\userItemFeatures.pickle")    
    userCategoryFeatures = _load_pickle("pickle\\userCategoryFeatures.pickle")
    return userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures

