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
#    itemFeatures         = _load_pickle("pickle\\itemFeatures.pickle")
    categoryFeatures    = _load_pickle("pickle\\categoryFeatures.pickle")
#    userItemFeatures     = _load_pickle("pickle\\userItemFeatures.pickle")    
    userCategoryFeatures = _load_pickle("pickle\\userCategoryFeatures.pickle")
    return userFeatures, categoryFeatures, userCategoryFeatures

def data_sort(train_user):
    train_user = train_user.sort(columns = ['user_category_pairs','D&H','user_item_pairs'])
    return train_user    

def str2num(train):
    header = train.columns
    for item in header:
        train[item] = train[item].apply(func = lambda x:round(float(x),3) if type(x) == str else x)
    return train