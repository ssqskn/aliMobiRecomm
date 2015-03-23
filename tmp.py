#coding=utf-8

import numpy as np
from utils import *
from data_import import *
from featureExtraction import *

if __name__ == '__main__':
    
    USE_SAMPLE_DATA = True
    ## data import
    if USE_SAMPLE_DATA: train_item, header_item, train_user, header_user = readSampleData()
    else:               train_item, header_item, train_user, header_user = readData(itemSize = 9999999, userSize = 99999999)
    
    train_user, header_user = columnProcess(train_user)
    train_user = listToDataFrame(train_user, header_user)
    
    train = data_for_train(train_user, days_involved = 15)
    
    print train.iloc[0,:]
    