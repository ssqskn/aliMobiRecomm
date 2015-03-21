#coding=utf-8

import time
from utils import dump_pickle, load_pickle
from data_process import *
from data_import import readData,readSampleData

START_TIME      = time.time()

if __name__ == '__main__':
    
    LOAD_FROM_PICKLE = False
    
    train_item, header_item, train_user, header_user = readSampleData()
#   train_item, header_item, train_user, header_user = readData(itemSize = 1000000, userSize = 10000000)
    train_user, header_user = columnProcess(train_user)
    train_user = listToDataFrame(train_user, header_user)   
    userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures, itemCategoryFeatures = feature_exaction(train_user, LOAD_FROM_PICKLE)


    print train_user[0:2]
    print userFeatures.iloc[0:2,:]