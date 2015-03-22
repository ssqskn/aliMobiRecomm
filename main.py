#coding=utf-8

from utils import *
from featureExaction import *
from data_import import *


if __name__ == '__main__':
    
    USE_SAMPLE_DATA  = True
    LOAD_FROM_PICKLE = True
    
    ## data import
    if USE_SAMPLE_DATA: train_item, header_item, train_user, header_user = readSampleData()
    else:               train_item, header_item, train_user, header_user = readData(itemSize = 9999999, userSize = 99999999)
    
    train_user, header_user = columnProcess(train_user)
    train_user = listToDataFrame(train_user, header_user)
    ## feature exaction
    userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures = feature_exaction(train_user, LOAD_FROM_PICKLE)
    train_user = featureCombination(train_user, userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures)
    ## preprocess










'''
    print train_user[0:2]
    print userFeatures.ix[0:2,:]
    print itemFeatures.ix[0:2,:]
    print userItemFeatures.ix[0:2,:]
    '''