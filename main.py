#coding=utf-8

import time
import pandas as pd
import numpy as np
from utils import shuffling, str2num_scale
from randomForest import fitRForest
from voting import majorityVoting
from sklearn import cross_validation
from sklearn.preprocessing import Imputer
from Prediction import createTestSet, createTestUserCategoryPairs

if __name__ == '__main__':
    
    USE_SAMPLE_DATA  = True
    SUBMIT           = False
    OVERSAMPLINGRATE = 3
    TRAINSETSIZE     = 70000
    
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
    del train['Unnamed: 0']; del train['user_id']; del train['item_id']
    del train['item_category']; del train['time']; del train['user_category_pairs']
    del train['Days']; del train['D&H']; del train['U&C_lastRecordTime']
    
    train = train.head(TRAINSETSIZE)
    ## over-sampling
    for i in range(OVERSAMPLINGRATE):
        train = train.append(train[train['if_pay'] == 1])
    ## shuffling
    train  = shuffling(train); 
    del train['rand']; del train['Unnamed: 0.1']; del train['userCount_u&c']; del train['userCount_u&c.1']
    ## convert to float and doing scaling
    train  = str2num_scale(train) 
    target = train['if_pay']; del train['if_pay']
    
    train.to_csv("debug\\train.csv")
    target.to_csv("debug\\target.csv")
    ## compute with NA depending on Imputer
    train  = Imputer().fit_transform(train)
    target = np.array(target)
    ##################
    #### training ####
    ##################
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
        '''
        ##using unused data
        val_kf = pd.read_csv(PATH + "train3.csv")
        del val_kf['Unnamed: 0']; del val_kf['user_id']; del val_kf['item_id']
        del val_kf['item_category']; del val_kf['time']; del val_kf['user_category_pairs']
        del val_kf['Days']; del val_kf['D&H']; del val_kf['U&C_lastRecordTime']
        del val_kf['Unnamed: 0.1']; del val_kf['userCount_u&c']; del val_kf['userCount_u&c.1']
        
        val_kf  = str2num_scale(val_kf)     
        target_val_kf = val_kf['if_pay']
        del val_kf['if_pay']
        
        val_kf  = Imputer().fit_transform(val_kf)
        target_val_kf = np.array(target_val_kf)    
        Nval = len(target_val_kf)
        
        np.savetxt("debug//valdationData"+str(rnd)+".csv",val_kf)
        np.savetxt("debug//val_target"+str(rnd)+".csv",target_val_kf)
        '''
        target_pred_rfs = [rfs_trained[i].predict(val_kf) for i in range(len(rfs_trained))] 
        target_pred     = majorityVoting(target_pred_rfs)
        
        '''
        np.savetxt("debug//pre_target"+str(rnd)+".csv",target_pred)
        '''
        
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
        test  = createTestSet(Days = 15)
        
        test_pred_rfs = [rfs[i].predict(test) for i in range(len(rfs))]
        test_pred = majorityVoting(test_pred_rfs)    
        ##save predictions
        np.savetxt("data\\submission\\submission_rf.csv",test_pred)
