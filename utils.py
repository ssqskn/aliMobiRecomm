#coding=utf-8
import pickle
import pandas as pd
import numpy as np
import MySQLdb

##dump pickle
def dump_pickle(dct,PATH):
    with open(PATH, 'wb') as f:
        pickle.dump(dct, f)
    f.close()
        
##reload pickle
def _load_pickle(PATH):
    with open(PATH, 'rb') as f:
        dct = pickle.load(f)
    return dct

def load_pickle():
    userFeatures         = _load_pickle("pickle\\userFeatures.pickle")
#    itemFeatures         = _load_pickle("pickle\\itemFeatures.pickle")
    categoryFeatures    = _load_pickle("pickle\\categoryFeatures.pickle")
#    userItemFeatures     = _load_pickle("pickle\\userItemFeatures.pickle")    
    userCategoryFeatures = _load_pickle("pickle\\userCategoryFeatures.pickle")
    return userFeatures, categoryFeatures, userCategoryFeatures

def data_sort(train_user):
    train_user = train_user.sort(columns = ['user_category_pairs','D&H','item_id'])
    return train_user    

def str2num_scale(train,TEST = False):
    header = train.columns
    for item in header:
        train[item] = train[item].apply(func = lambda x:round(float(x),3) if type(x) == str else x)
        if not TEST:
            #if not item == 'if_pay':
            if not item == 22:
                itemAvg     = train[item].mean(axis = 0)
                itemStd     = train[item].std(axis = 0)
                train[item] = train[item].apply(func = lambda x: (x - itemAvg) * 1.0/itemStd)
        else:
            itemAvg     = train[item].mean(axis = 0)
            itemStd     = train[item].std(axis = 0)
            train[item] = train[item].apply(func = lambda x: (x - itemAvg) * 1.0/itemStd)
        train[item].fillna(0)
    return train

def shuffling(train):
    Idx = train.index.values
    rands = np.random.random_sample(len(Idx))
    train['rand'] = rands
    train = train.sort_index(by = ['rand'])
    header = train.columns
    train  = pd.DataFrame(np.array(train), columns = header)
    del train['rand']
    return train

def sqlConnect():
    connect = MySQLdb.connect(host = 'localhost', port = 3306, user = 'root', passwd = 'Aa323232', db   = 'scott')    
    cur     = connect.cursor()
    return cur, connect