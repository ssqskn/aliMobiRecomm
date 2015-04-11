#coding=utf-8

import time
import math
import pandas as pd
import numpy as np
from utils import shuffling, str2num_scale, sqlConnect
from randomForest import fitRForest
from voting import majorityVoting
from sklearn import cross_validation
from sklearn.preprocessing import Imputer

def trainFunc(train,target,kfold,Nrfs,params,SUBMIT,PosTRAINSETSIZE,NegTRAINSETSIZE):
    trainTime = time.time()
    ##################
    #### training ####
    ##################
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
        
        print "Start training............: ",round((time.time() - START_TIME),2)   
        ##train
        rfs_trained = fitRForest(train_kf, target_kf, Nrfs, params)
    
        print "Stop training.............: ",round((time.time() - START_TIME),2)   
        np.savetxt("submission\\feature_importances.txt",rfs_trained[0].feature_importances_)
        
        if rnd == 1: 
            rfs.append(rfs_trained)
 
        target_pred_rfs = [rfs_trained[i].predict(val_kf) for i in range(len(rfs_trained))] 
        target_pred     = majorityVoting(target_pred_rfs)
        
        print "Prediction time...........: ",round((time.time() - START_TIME),2)   
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
    f = open("debug\\params.txt",'a')
    n,m,l = params[0]
    f.write("(1) TrainSetSize: pos-"+str(PosTRAINSETSIZE)+", neg-"+str(NegTRAINSETSIZE)+"  (2) ntrees: "+str(n)+", maxfeatures: "+
            str(m)+", leafsize: "+str(l)+"  --------- CorrectionRate: "+str((100 - round(validationError*100,2)))+"% ,"+" Time:"+
            str(round((time.time()-trainTime),2))+"\n")
    f.close()
    
    return rfs


if __name__ == '__main__':
    
    SUBMIT           = True
    PosTRAINSETSIZE  = 3252   ##max 3252
    NegTRAINSETSIZE  = 210000
    PREDSETSIZE      = 533000  ##total number:532897 in testForSubmit
    
    params = [(40,40,20)]    #ntree, maxfea, leafsize of random forest
    Nrfs   = 3              #number of random rfs
    kfold  = 3   
    
    if (NegTRAINSETSIZE/min([PosTRAINSETSIZE,3252])) >= 8:
        OVERSAMPLINGRATE = int(math.log((NegTRAINSETSIZE * 1.0/8 / min([PosTRAINSETSIZE,3252])),2))
    else: OVERSAMPLINGRATE = 0
    
    START_TIME = time.time()
    ## data import
    cur,connect = sqlConnect()
    count = cur.execute("select * from Train where target = 4 LIMIT 0," + str(PosTRAINSETSIZE))
    train = pd.DataFrame(list(cur.fetchall()))      ###target:col = 22
    
    count = cur.execute("select * from Train where target is null LIMIT 0," + str(NegTRAINSETSIZE))
    train = train.append(list(cur.fetchall()))
    
    cur.close
    connect.close
    print "Read train set............: ",round((time.time() - START_TIME),2)     
    
    train[22] = train[22].replace(4,1)           ## target - buy:1, not buy:0
    del train[0]; del train[1]; del train[2]
    del train[3]; del train[4]
    ## over-sampling
    for i in range(OVERSAMPLINGRATE):
        train = train.append(train[train[22] == 1])
    ## shuffling
    train  = shuffling(train); 
    train  = train.fillna(0);
    ## convert to float and doing scaling
    train  = str2num_scale(train) 
    target = train[22]
    del train[22]
    ## additional features
    
    
    
    

    train.to_csv("debug\\train.csv")
    target.to_csv("debug\\target.csv")
    
    print "Train set process............: ",round((time.time() - START_TIME),2)   
    
    ## compute with NA depending on Imputer
    train  = Imputer().fit_transform(train)
    target = np.array(target)
    
    
    for n in [40]:
        for m in [40]:
            for l in [20]:
                params = [(n,m,l)]
                rfs = trainFunc(train,target,kfold,Nrfs,params,SUBMIT,PosTRAINSETSIZE,NegTRAINSETSIZE)
    
    ## predicting
    if SUBMIT == True:
        del train
        del target
        ## data import
        cur,connect = sqlConnect()
        count = cur.execute("select * from testForSubmit LIMIT 0," + str(PREDSETSIZE))
        pred = pd.DataFrame(list(cur.fetchall())) 
        cur.close
        connect.close
        
        userCateIdx = pd.DataFrame(list(pred[1]), columns=['user_id'])
        userCateIdx['item_id'] = pred[2]
        del pred[0]; del pred[1]; del pred[2]
        del pred[3]; del pred[4]
        
        pred  = pred.fillna(0);
        pred  = str2num_scale(pred,TEST=True)
        ## additional features - corresponding to train set
        
        
        
         
        pred  = Imputer().fit_transform(pred)
        
        test_pred_rfs = [rfs[i].predict(pred) for i in range(len(rfs))]
        test_pred = majorityVoting(test_pred_rfs)   
        
        print "Prediction time............: ",round((time.time() - START_TIME),2) 
        ##save predictions
        userCateIdx['pred'] = list(test_pred)
        userCateIdx.to_csv("submission\\prediction_rf.csv")
        np.savetxt("submission\\pred.csv",test_pred)
        ##save submit result
        userCateIdx[userCateIdx['pred'] == 1].to_csv("submission\\tianchi_mobile_recommendation_predict.csv")