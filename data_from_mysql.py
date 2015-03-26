#coding=utf-8
import time
import MySQLdb
import numpy as np
import pandas as pd
from utils import dump_pickle
from utils import data_sort
from preprocess import data_preprocess_1
from featureExtraction import user_feature_process

def sqlConnect():
    connect = MySQLdb.connect(host = 'localhost', port = 3306, user = 'root', passwd = 'Aa323232', db   = 'scott')    
    cur     = connect.cursor()
    return cur, connect







if __name__ == '__main__':
    
    FROM_PICKLE = False
    
    if FROM_PICKLE == False:
        cur, connect = sqlConnect()
        '''
        count = cur.execute("select user_category_pairs,count(*) from aliMobRec group by user_category_pairs")
        user_category_pairs = np.array(cur.fetchall())
        ## count = 903136, total records around 12312542; 36000 per step

        step = 9031    ####160s per step, around 120000
        rnd  = count / step    
        for j in range(rnd):
            print "................ User-category processing start round ",j,"  .............."
            startTime = time.time()
            for i in range(step):
                cur.execute("select user_id,item_id,behavior_type,item_category,r_time,user_category_pairs,Days,D_H \
                            from aliMobRec where user_category_pairs = '" + user_category_pairs[(j * step +i),0] + "'")   
                if i == 0:
                    data = pd.DataFrame(list(cur.fetchall()), columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','D&H'])
                else:
                    data = data.append(pd.DataFrame(list(cur.fetchall()),columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','D&H']))
        
            print "round ",j,": ",(time.time() - startTime)
            ## data_process
            data = data_sort(data)
            ## generate train set
            train = data_preprocess_1(data, 15)
            ## save train data
            train.to_csv("pickle//train_"+ str(j) +".csv")
            dump_pickle(train, "pickle//train_"+ str(j) +".pickle")
            del train; del data
        '''
        #### user features  ---------count == 10000
        count = cur.execute("select user_id,count(*) from aliMobRec group by user_id")
        users = np.array(cur.fetchall())
        step  = 100
        rnd   = count / step
        for j in range(rnd):
            print "........... User start round ",j," ................"
            startTime = time.time()
            
            for i in range(step):
                cur.execute("select user_id,item_id,behavior_type,item_category,r_time,user_category_pairs,Days,D_H \
                            from aliMobRec where user_id = '" + users[(j * step +i),0] + "'")   
                if i == 0:
                    data = pd.DataFrame(list(cur.fetchall()), columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','D&H'])
                else:
                    data = data.append(pd.DataFrame(list(cur.fetchall()),columns = ['user_id','item_id','behavior_type','item_category','time','user_category_pairs','Days','D&H']))
            
            data['Days'] = data['Days'].apply(lambda x:int(x))
            print "round ",j,": ",(time.time() - startTime)
            ## data_process
            userFeatures = user_feature_process(data)
            ## save train data
            userFeatures.to_csv("pickle//userFeatures_"+ str(j) +".csv")
            dump_pickle(userFeatures, "pickle//userFeatures_"+ str(j) +".pickle")
            del userFeatures; del data
        
        
        
        
        
        
        
    else:
        pass    
        
         