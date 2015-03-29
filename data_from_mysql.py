#coding=utf-8
import time
import MySQLdb
import numpy as np
import pandas as pd
from utils import dump_pickle, sqlConnect
from utils import data_sort, _load_pickle
from featureExtraction import user_feature_process, category_feature_process, userCategory_feature_process,\
    user_feature_cal, category_feature_cal, userCategoryFeatureCal, item_feature_process, item_feature_cal, \
    featureCombination, data_preprocess_1
from Prediction import createTestUserCategoryPairs

def featuresCombination():
    PATH = "pickle\\"
    
    print "----------implementing user features combination----------"
    for i in range(100):
        if i == 0:
            userFeatures = _load_pickle(PATH + "userFeatures_" + str(i) +".pickle")
        else:
            userFeatures = userFeatures.append(_load_pickle(PATH + "userFeatures_" + str(i) +".pickle"))
    userFeatures = user_feature_cal(userFeatures)
    userFeatures.to_csv(PATH + "userFeatures_ALL.csv")
    dump_pickle(userFeatures, PATH + "userFeatures_ALL.pickle")
    del userFeatures
    
    print "----------implementing category features combination----------"    
    for i in range(99):
        if i == 0:
            categoryFeatures = _load_pickle(PATH + "categoryFeatures_" + str(i) +".pickle")
        else:
            categoryFeatures = categoryFeatures.append(_load_pickle(PATH + "categoryFeatures_" + str(i) +".pickle"))
    categoryFeatures = category_feature_cal(categoryFeatures)
    categoryFeatures.to_csv(PATH + "categoryFeatures_ALL.csv")
    dump_pickle(categoryFeatures, PATH + "categoryFeatures_ALL.pickle")
    del categoryFeatures
    
    '''
    print "----------implementing item features combination----------"    
    for i in range(1000):
        print i
        if i == 0:
            itemFeatures = _load_pickle(PATH + "itemFeatures_" + str(i) +".pickle")
        else:
            itemFeatures = itemFeatures.append(_load_pickle(PATH + "itemFeatures_" + str(i) +".pickle"))
    #itemFeatures = item_feature_cal(itemFeatures)
    itemFeatures.to_csv(PATH + "itemFeatures_ALL.csv")
    dump_pickle(itemFeatures, PATH + "itemFeatures_ALL.pickle")
    del itemFeatures
    '''
    
    for i in range(100):
        print "----------implementing user-category features processing------ round: ",i,"----------"
        userCategoryFeatures = _load_pickle(PATH + "userCategoryFeatures_" + str(i) +".pickle")
        userCategoryFeatures = userCategoryFeatureCal(userCategoryFeatures)
        train = _load_pickle(PATH + "train_" + str(i) +".pickle")
        header = train.columns
        train = train.merge(userCategoryFeatures, left_on = 'user_category_pairs', right_on = 'user_category_pairs', how = 'left')
        header = header.append(userCategoryFeatures.columns[1::] + '_u&c'); train.columns = header      
        
        dump_pickle(train, PATH + "train_I_" + str(i) +".pickle") 
        train.to_csv(PATH + "train_I_" + str(i) +".csv")
    del userCategoryFeatures; del train
    
    
def generateTrain():
    PATH = 'pickle\\'
    
    for i in range(100):
        print "----------implementing train set processing------ round: ",i,"---------"
        train = _load_pickle(PATH + "train_I_" + str(i) +".pickle")
        userCategoryFeatures = _load_pickle(PATH + "userCategoryFeatures_" + str(i) +".pickle")
        userFeatures = _load_pickle(PATH + "userFeatures_ALL.pickle")
        categoryFeatures = _load_pickle(PATH + "categoryFeatures_ALL.pickle")
        
        train = featureCombination(train, userFeatures, categoryFeatures,  userCategoryFeatures)
        dump_pickle(train, PATH + "train_part_" + str(i) +".pickle") 
        train.to_csv(PATH + "train_part_" + str(i) +".csv")
        del train; del userCategoryFeatures; del userFeatures; del categoryFeatures
    
    print "-------------------combination of train data-------------------"
    for r in range(4):
        for i in range(25):
            if i == 0:
                train = pd.read_csv(PATH + "train_part_" + str(r*20 + i) + ".csv")
            else:
                train = train.append(pd.read_csv(PATH + "train_part_" + str(r*20 + i) + ".csv"))
        train.fillna(0)
        train.replace('inf', -999)
        train.to_csv(PATH + "train" + str(r) + ".csv")
        del train


if __name__ == '__main__':
    
    FROM_PICKLE = True
    
    if FROM_PICKLE == False:
        cur, connect = sqlConnect()
        
        count = cur.execute("select user_category_pairs,count(*) from aliMobRec group by user_category_pairs")
        user_category_pairs = np.array(cur.fetchall())
        ## count = 903136, total records around 12312542; 36000 per step

        step = 9031    ####160s per step, around 120000
        rnd  = count / step    
        for j in range(rnd):
            print "................ User-category train set processing start round ",j,"  .............."
            startTime = time.time()
            
            for i in range(step):
                cur.execute("select user_id,item_id,behavior_type,item_category,r_time,user_category_pairs,Days,hours,D_H \
                            from aliMobRec where user_category_pairs = '" + user_category_pairs[(j * step +i),0] + "'")   
                if i == 0:
                    data = pd.DataFrame(list(cur.fetchall()), columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','HH','D&H'])
                else:
                    data = data.append(pd.DataFrame(list(cur.fetchall()),columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','HH','D&H']))
        
            print "round ",j,": ",(time.time() - startTime)
            
            ## user-category features
            data_1 = data
            data_1['Days'] = data_1['Days'].apply(lambda x:int(x))
            data_1['HH'] = data_1['HH'].apply(lambda x:int(x))

            ## data_process
            userCategoryFeatures = userCategory_feature_process(data_1)
            ## save train data
            userCategoryFeatures.to_csv("pickle//userCategoryFeatures_"+ str(j) +".csv")
            dump_pickle(userCategoryFeatures, "pickle//userCategoryFeatures_"+ str(j) +".pickle")
            del userCategoryFeatures
            del data_1
            ## data_process
            del data['HH']
            data = data_sort(data)
            ## generate train set
            train = data_preprocess_1(data, 15, TEST = False)
            ## save train data
            train.to_csv("pickle//train_"+ str(j) +".csv")
            dump_pickle(train, "pickle//train_"+ str(j) +".pickle")
            del train; del data
            
            test = []
            for i in range(step): 
                cur.execute("select user_id,item_category,user_category_pairs from userCategoryForPred\
                            where user_category_pairs = '" + user_category_pairs[(j * step +i),0] + "'")  
                if i == 0 or (not len(test)):
                    tmp = list(cur.fetchall())
                    if len(tmp):
                        test = pd.DataFrame(tmp, columns = ['user_id','item_category','user_category_pairs'])
                else:
                    tmp = list(cur.fetchall())
                    if len(tmp):
                        test = test.append(pd.DataFrame(tmp,columns = ['user_id','item_category','user_category_pairs']))
            if not len(test) == 0: 
                test.to_csv("pickle//test//test_ori_" + str(j) + ".csv")
                dump_pickle(test, "pickle//test//test_ori_" + str(j) + ".pickle")
            del test
            print "round ",j,": ",(time.time() - startTime)
        del user_category_pairs
        
        #### user features  ---------count == 10000
        count = cur.execute("select user_id,count(*) from aliMobRec group by user_id")
        users = np.array(cur.fetchall())
        step  = 100   ##  8 seconds per round
        rnd   = count / step
        for j in range(rnd):
            print "........... User start round ",j," ................"
            startTime = time.time()
            
            for i in range(step):
                cur.execute("select user_id,item_id,behavior_type,item_category,r_time,user_category_pairs,Days,hours,D_H \
                            from aliMobRec where user_id = '" + users[(j * step +i),0] + "'")   
                if i == 0:
                    data = pd.DataFrame(list(cur.fetchall()), columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','HH','D&H'])
                else:
                    data = data.append(pd.DataFrame(list(cur.fetchall()),columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','HH','D&H']))
            
            data['Days'] = data['Days'].apply(lambda x:int(x))
            data['HH'] = data['HH'].apply(lambda x:int(x))

            print "round ",j,": ",(time.time() - startTime)
            ## data_process
            userFeatures = user_feature_process(data)
            ## save train data
            userFeatures.to_csv("pickle//userFeatures_"+ str(j) +".csv")
            dump_pickle(userFeatures, "pickle//userFeatures_"+ str(j) +".pickle")
            del userFeatures; del data
        del users
        
        #### category features  ---------count == 8898
        count = cur.execute("select item_category,count(*) from aliMobRec group by item_category")
        ctgs = np.array(cur.fetchall())
        step  = 89          ## 20 seconds per round
        rnd   = count / step
        for j in range(rnd):
            print "........... Category start round ",j," ................"
            startTime = time.time()
            
            for i in range(step):
                if (j * step + i) >= count:
                    break
                else:
                    cur.execute("select user_id,item_id,behavior_type,item_category,r_time,user_category_pairs,Days,hours,D_H \
                                from aliMobRec where item_category = '" + ctgs[(j * step +i),0] + "'")   
                    if i == 0:
                        data = pd.DataFrame(list(cur.fetchall()), columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','HH','D&H'])
                    else:
                        data = data.append(pd.DataFrame(list(cur.fetchall()),columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','HH','D&H']))
            
            data['Days'] = data['Days'].apply(lambda x:int(x))
            data['HH'] = data['HH'].apply(lambda x:int(x))

            print "round ",j,": ",(time.time() - startTime)
            ## data_process
            categoryFeatures = category_feature_process(data)
            ## save train data
            categoryFeatures.to_csv("pickle//categoryFeatures_"+ str(j) +".csv")
            dump_pickle(categoryFeatures, "pickle//categoryFeatures_"+ str(j) +".pickle")
            del categoryFeatures; del data
        del ctgs
        '''
        #### item features  ---------count ==  2914411
        count = cur.execute("select item_id,count(*) from aliMobRec group by item_id")
        items = np.array(cur.fetchall())
        step  = 2914          ##  8 seconds per round
        rnd   = count / step
        for j in range(rnd):
            print "........... Item start round ",j," ................"
            startTime = time.time()
            
            for i in range(step):
                if (j * step + i) >= count:
                    break
                else:
                    cur.execute("select user_id,item_id,behavior_type,item_category,r_time,user_category_pairs,Days,hours,D_H \
                                from aliMobRec where item_id = '" + items[(j * step +i),0] + "'")   
                    if i == 0:
                        data = pd.DataFrame(list(cur.fetchall()), columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','HH','D&H'])
                    else:
                        data = data.append(pd.DataFrame(list(cur.fetchall()),columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','HH','D&H']))
            
            data['Days'] = data['Days'].apply(lambda x:int(x))
            data['HH'] = data['HH'].apply(lambda x:int(x))

            print "round ",j,": ",(time.time() - startTime)
            ## data_process
            itemFeatures = item_feature_process(data)
            ## save train data
            itemFeatures.to_csv("pickle//itemFeatures_"+ str(j) +".csv")
            dump_pickle(itemFeatures, "pickle//itemFeatures_"+ str(j) +".pickle")
            del itemFeatures; del data
        del items
        '''
        featuresCombination()
        
        createTestUserCategoryPairs()
        
    else:
        generateTrain()  