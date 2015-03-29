#coding=utf-8

import numpy as np
import pandas as pd
from utils import _load_pickle, dump_pickle, str2num_scale, data_sort, sqlConnect
from featureExtraction import featureCombination, data_preprocess_1
from sklearn.preprocessing import Imputer

def createTestUserCategoryPairs():
    ## create user-category pairs
    cur, connect = sqlConnect()
    
    cur.execute("select distinct(user_id) from aliMobRec")
    users = np.array(cur.fetchall())

    cur.execute("select distinct(item_category) from aliMobRecItemForPred")
    category = np.array(cur.fetchall())
    
    lenUser = len(users)
    lenCate = len(category)
    
    cur.execute("drop table userCategoryForPred")
    cur.execute("create table userCategoryForPred(user_id varchar(25),\
                item_category varchar(25),user_category_pairs varchar(50))")
    
    for j in range(lenCate):
        values  = []
        if j % 100 == 0: print "insert %d categories: "%j
        for i in range(lenUser):
            values.append((str(users[i][0]),str(category[j][0].replace('\r',''))))
        cur.executemany("insert into userCategoryForPred(user_id,item_category) values(%s,%s)",values)
        connect.commit()
        del values
    cur.close()
    connect.close()

def createTestSet(Days):
    PATH = "pickle\\test\\"
    for i in range(100):
        print "-----------------------creating test set -- round: ",i,"-----------------------"
        test = _load_pickle(PATH + "test_ori_" + str(i) +".pickle"); del test['user_id']; del test['item_category']
        data = _load_pickle("pickle\\" + "train_"+ str(i) +".pickle")
        
        data['flag'] = 0
        for i in range(len(data)):
            if len(test[test['user_category_pairs'] == data['user_category_pairs'][i]]):
                data['flag'][i] = 1
        else:   data['flag'][i] = 0
        
        data = data[data['flag'] == 1]; del data['flag']
        data['D&H']  = 32
        del data['U&C_lastRecordTime']
        del data['if_pay']
        
        data = data_sort(data)
        data = data_preprocess_1(data, 15, TEST = True)
        data.to_csv("pickle//test//test_"+ str(i) +".csv")
        dump_pickle(data, "pickle//test//test_"+ str(i) +".pickle")
    del data; del test
        
    for i in range(100):
        print "----------implementing test set processing------ round: ",i,"---------"
        test = _load_pickle(PATH + "test_" + str(i) +".pickle")
        userCategoryFeatures = _load_pickle("pickle\\" + "userCategoryFeatures_" + str(i) +".pickle")
        userFeatures = _load_pickle("pickle\\" + "userFeatures_ALL.pickle")
        categoryFeatures = _load_pickle("pickle\\" + "categoryFeatures_ALL.pickle")
        
        test = featureCombination(test, userFeatures, categoryFeatures,  userCategoryFeatures)
        dump_pickle(test, PATH + "test" + str(i) +".pickle") 
        test.to_csv(PATH + "test_" + str(i) +".csv")
        del test; del userCategoryFeatures; del userFeatures; del categoryFeatures
    
    print "-------------------combination of test data-------------------"
    for i in range(100):
        if i == 0:
            test = pd.read_csv(PATH + "test" + str(i) + ".csv")
        else:
            test = test.append(pd.read_csv(PATH + "test_" + str(i) + ".csv"))
    test.fillna(0)
    test.replace('inf', -999)
    test.to_csv(PATH + "test.csv")
    
    del test['Unnamed: 0']; del test['user_id']; del test['item_id']
    del test['item_category']; del test['time']; del test['user_category_pairs']
    del test['Days']; del test['D&H']; del test['U&C_lastRecordTime']
    del test['rand']; del test['userCount_u&c']; del test['userCount_u&c.1']
    
    test  = str2num_scale(test) 
    del test['if_pay']
    
    test  = Imputer().fit_transform(test)
    return test
