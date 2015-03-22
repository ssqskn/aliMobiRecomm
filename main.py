#coding=utf-8

import numpy as np
from utils import *
from data_import import *
from featureExtraction import *
from randomForest import fitRForest
from voting import majorityVoting
from sklearn import cross_validation


if __name__ == '__main__':
    
    USE_SAMPLE_DATA  = True
    LOAD_FROM_PICKLE = True
    SUBMIT           = False
    
    params = [(30,5,20)]    #ntree, maxfea, leafsize of random forest
    Nrfs   = 2              #number of random rfs
    kfold  = 3   
     
    ## data import
    if USE_SAMPLE_DATA: train_item, header_item, train_user, header_user = readSampleData()
    else:               train_item, header_item, train_user, header_user = readData(itemSize = 9999999, userSize = 99999999)
    
    train_user, header_user = columnProcess(train_user)
    train_user = listToDataFrame(train_user, header_user)
    ## feature exaction
    userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures = feature_exaction(train_user, LOAD_FROM_PICKLE)
    train_user = featureCombination(train_user, userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures)

#   train_user.to_csv("data_tmp//feaExacted.csv")
#   dump_pickle(train_user, "pickle//feaCombined.pickle")
    
    ## preprocess
    






'''
    ## training
    train      = []
    target     = []
    
    rfs        = []
    maxCorrRfs = 0
    ##k-fold division
    validationError = 0.0
    rnd             = 0
    kf = cross_validation.KFold(len(target), kfold, indices=False)
    
    for train_ix,val_ix in kf:                      ##k-fold cross-validation
        ##if doing test, just run one round
        if SUBMIT == True:
            train_ix = np.ones(len(target), dtype=bool)
            val_ix   = train_ix
            
        rnd += 1
        train_kf ,val_kf        = train[train_ix],train[val_ix]
        target_kf,target_val_kf = target[train_ix],target[val_ix]
        
        Ntrain = len(target_kf)
        Nval   = len(target_val_kf)        
        ##train
        rfs_trained = fitRForest(train_kf, target_kf, Nrfs, params)
        
        if rnd == 1: 
            rfs.append(rfs_trained)
        ##predict training data
        target_pred_rfs = [rfs_trained[i].predict(val_kf) for i in range(len(rfs_trained))] 
        target_pred     = majorityVoting(target_pred_rfs)   
        ##calculate error
        rfs_error       = (target_pred - np.array(target_val_kf))**2
        
        if maxCorrRfs < ((Nval-sum(rfs_error))*1.0/Nval):
            maxCorrRfs = (Nval-sum(rfs_error))*1.0/Nval
            rfs = rfs_trained
        
        errorRate        = sum(rfs_error)*1.0/Nval
        validationError += errorRate
        print "Fold%d: rfs correct rate is %.3f"%(rnd, (1-errorRate))

        if SUBMIT == True: break

    validationError = validationError * 1.0 / rnd
    
    log = ['----Parameters: ', params, '----ErrorRate: ', validationError, '----']
    print log
    
    
    ## predicting
    if SUBMIT == True:
        test  = []
        
        test_pred_rfs = [rfs[i].predict(test) for i in range(len(rfs))]
        test_pred = majorityVoting(test_pred_rfs)    
        ##save predictions
        np.savetxt("data\\submission_rf.csv",test_pred)
    '''