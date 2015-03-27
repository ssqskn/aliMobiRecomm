#coding=utf-8

import time
import pandas as pd
import numpy as np
from utils import str2num, shuffling
from randomForest import fitRForest
from voting import majorityVoting
from sklearn import cross_validation


if __name__ == '__main__':
    
    USE_SAMPLE_DATA  = True
    LOAD_FROM_PICKLE = False
    SUBMIT           = False
    OVERSAMPLINGRATE = 5
    
    params = [(20,5,20)]    #ntree, maxfea, leafs ize of random forest
    Nrfs   = 3              #number of random rfs
    kfold  = 3   
    
    ## data import
    PATH = "pickle\\"
    if USE_SAMPLE_DATA: 
        train = pd.read_csv(PATH + "train0.csv")
    else:               
        train = pd.read_csv(PATH + "train0.csv")
        train.append(pd.read_csv(PATH + "train1.csv"))
#       train.append(pd.read_csv(PATH + "train2.csv"))
#       train.append(pd.read_csv(PATH + "train3.csv"))
        
    ## over-sampling
    for i in range(OVERSAMPLINGRATE):
        train = train.append(train[train['if_pay'] == 1])
    train  = str2num(train) 
    
    target = train['if_pay']; del train['if_pay']

    train  = np.array(train)
    target = np.array(target)
    
    train, target = shuffling(train, target)  

    ## training
    START_TIME = time.time()
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
        
        print "Start training............: ",(time.time() - START_TIME)         
        ##train
        rfs_trained = fitRForest(train_kf, target_kf, Nrfs, params)
        print "Stop training.............: ",(time.time() - START_TIME)
        
        if rnd == 1: 
            rfs.append(rfs_trained)
        ##predict training data
        target_pred_rfs = [rfs_trained[i].predict(val_kf) for i in range(len(rfs_trained))] 
        target_pred     = majorityVoting(target_pred_rfs)   
        print "Prediction time...........: ",(time.time() - START_TIME)
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
    
    print '[----Parameters: ', params, '----ErrorRate: ', validationError * 100, '%----]'
    
    
    
    ## predicting
    if SUBMIT == True:
        test  = []
        
        test_pred_rfs = [rfs[i].predict(test) for i in range(len(rfs))]
        test_pred = majorityVoting(test_pred_rfs)    
        ##save predictions
        np.savetxt("data\\submission\\submission_rf.csv",test_pred)
