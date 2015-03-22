#coding=utf-8
import pandas as pd
import time
from utils import dump_pickle, load_pickle
from pandas import DataFrame,pivot_table
from data_import import readData,readSampleData

##convert list to data frame
def listToDataFrame(data, header):
    return DataFrame(data, columns=header)

##split time, add pair indicators
def columnProcess(data):
    count = len(data)
    for i in range(count):
        ##split time into YYYY MM DD HH   
        monthDaysDict = {'1':0,'2':31,'3':59,'4':90,'5':120,'6':151,'7':181,
                         '8':212,'9':243,'10':273,'11':304,'12':334}
        timeSplit = data[i][5].split('-')
        timeSplit2 = timeSplit[2].split(' ')
        data[i].append(int(timeSplit[0]));      data[i].append(int(timeSplit[1]))
        data[i].append(int(timeSplit2[0]));     data[i].append(int(timeSplit2[1]))    
        data[i].append((int(timeSplit[0])-2014)*365 + monthDaysDict[timeSplit[1]] + int(timeSplit2[0]) - 304 - 17) ##count from 20141118    
        ##add user_item pair and user_category pair
        data[i].append((data[i][0],data[i][1]))
        data[i].append((data[i][0],data[i][4]))
        data[i].append((data[i][1],data[i][4]))
        
    header = ['user_id','item_id','behavior_type','user_geohash','item_category',
              'time','YYYY','MM','DD','HH','Days','user_item_pairs','user_category_pairs','item_category_pairs']
        
    return data, header
##exact features from train data
def feature_exaction(data, LOAD_FROM_PICKLE):
#   userFeatures = DataFrame(list(set(data['user_id'])), columns = ['user_id'])
#   userFeatures = DataFrame(pd.unique(data['user_id']), columns = ['user_id'])
    START_TIME = time.time()
    '''   
    userDict = {}
    itemDict = {}
    categoryDict = {}
    userItemDict = {}
    userCategoryDict = {}

    for i in range(count):
        if not userDict.has_key(data.ix[i,'user_id']):
            userDict[data.ix[i,'user_id']]             = {}
        if not itemDict.has_key(data.ix[i,'item_id']):
            itemDict[data.ix[i,'item_id']]             = {}
        if not categoryDict.has_key(data.ix[i,'item_category']):
            categoryDict[data.ix[i,'item_category']]   = {}
        if not userItemDict.has_key(data.ix[i,'user_item_pairs']):
            userItemDict[data.ix[i,'user_item_pairs']] = {}
        if not userCategoryDict.has_key(data.ix[i,'user_category_pairs']):
            userCategoryDict[data.ix[i,'user_category_pairs']]   = {}

    #############################
    ###exact features for user###
    #############################
    for i in range(count):
        ##records
        if not userDict[data.ix[i,'user_id']].has_key('records'):
            userDict[data.ix[i,'user_id']]['records'] = 1
        else: userDict[data.ix[i,'user_id']]['records'] += 1
        
    ##items&categories per user
    for item in userDict:
        userDict[item]['itemCount'] = len(pd.unique(data[data['user_id'] == item]['item_id']))
        userDict[item]['categoryCount'] = len(pd.unique(data[data['user_id'] == item]['item_category']))
        userDict[item]['OnlineDays'] = len(pd.unique(data[data['user_id'] == item]['Days']))
        userDict[item]['Period']     = max(data[data['user_id'] == item]['Days']) - min(data[data['user_id'] == item]['Days'])
    
    dump_pickle(userDict, "pickle\\userDict.pickle")
    dump_pickle(itemDict, "pickle\\itemDict.pickle")
    dump_pickle(categoryDict, "pickle\\categoryDict.pickle")
    dump_pickle(userItemDict, "pickle\\userItemDict.pickle")    
    dump_pickle(userCategoryDict, "pickle\\userCategoryDict.pickle")       
    
    userDict, itemDict, categoryDict, userItemDict, userCategoryDict = load_pickle() 
      
    return userDict, itemDict, categoryDict, userItemDict, userCategoryDict
       
    '''   
    if LOAD_FROM_PICKLE:
        userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures = load_pickle()
    else:
        ## features of user
        userFeaCol   = ['user_id']
        userFeatures = DataFrame(data['user_id'].unique(), columns = userFeaCol)
        userFeatures = userFeatures.merge(DataFrame(data.pivot_table(values = 'item_id', rows='user_id', aggfunc = lambda x:len(x.unique()))),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('itemCount')
        userFeatures = userFeatures.merge(DataFrame(data.pivot_table(values = 'item_id', rows='user_id', aggfunc = len)),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('records')
        userFeatures = userFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='user_id', aggfunc = lambda x:len(x.unique()))),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('Online_Days')
        userFeatures = userFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='user_id', aggfunc = lambda x: max(x) - min(x) + 1)),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('Period')
        userFeatures = userFeatures.merge(DataFrame(data[data['behavior_type'] == '1'].pivot_table(values = 'item_id', rows='user_id', aggfunc = len)),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('Behavior_1'); userFeatures.columns = userFeaCol
        userFeatures = userFeatures.merge(DataFrame(data[data['behavior_type'] == '2'].pivot_table(values = 'item_id', rows='user_id', aggfunc = len)),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('Behavior_2'); userFeatures.columns = userFeaCol
        userFeatures = userFeatures.merge(DataFrame(data[data['behavior_type'] == '3'].pivot_table(values = 'item_id', rows='user_id', aggfunc = len)),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('Behavior_3'); userFeatures.columns = userFeaCol
        userFeatures = userFeatures.merge(DataFrame(data[data['behavior_type'] == '4'].pivot_table(values = 'item_id', rows='user_id', aggfunc = len)),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('Behavior_4'); userFeatures.columns = userFeaCol
        userFeatures = userFeatures.merge(DataFrame(data[((data['HH'] >= 18) & (data['HH'] <= 24))].pivot_table(values = 'item_id', rows='user_id', aggfunc = len)),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('records_evening'); userFeatures.columns = userFeaCol
        userFeatures = userFeatures.merge(DataFrame(data[((data['HH'] >= 0) & (data['HH'] <= 7))].pivot_table(values = 'item_id', rows='user_id', aggfunc = len)),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('records_midnight'); userFeatures.columns = userFeaCol
        userFeatures = userFeatures.merge(DataFrame(data[((data['HH'] >= 13) & (data['HH'] <= 17))].pivot_table(values = 'item_id', rows='user_id', aggfunc = len)),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('records_afternoon'); userFeatures.columns = userFeaCol
        userFeatures = userFeatures.merge(DataFrame(data[((data['HH'] >= 8) & (data['HH'] <= 12))].pivot_table(values = 'item_id', rows='user_id', aggfunc = len)),
                                          left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('records_morning'); userFeatures.columns = userFeaCol   

        print "Time for user feature exaction: %.3f" % (time.time() - START_TIME)

        ## features of item
        itemFeaCol = ['item_id']
        itemFeatures = DataFrame(data['item_id'].unique(), columns = itemFeaCol)
        itemFeatures = itemFeatures.merge(DataFrame(data.pivot_table(values = 'user_id', rows='item_id', aggfunc = lambda x:len(x.unique()))),
                                          left_on = 'item_id', right_index = True); itemFeaCol.append('userCount'); itemFeatures.columns = itemFeaCol
        itemFeatures = itemFeatures.merge(DataFrame(data.pivot_table(values = 'user_id', rows='item_id', aggfunc = len)),
                                          left_on = 'item_id', right_index = True); itemFeaCol.append('Records'); itemFeatures.columns = itemFeaCol
        itemFeatures = itemFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='item_id', aggfunc = lambda x:max(x) - min(x) + 1)),
                                          left_on = 'item_id', right_index = True); itemFeaCol.append('Period'); itemFeatures.columns = itemFeaCol
        itemFeatures = itemFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='item_id', aggfunc = lambda x:len(x.unique()))),
                                          left_on = 'item_id', right_index = True); itemFeaCol.append('ClickDays'); itemFeatures.columns = itemFeaCol        
        itemFeatures = itemFeatures.merge(DataFrame(data[data['behavior_type'] == '1'].pivot_table(values = 'user_id', rows='item_id', aggfunc = len)),
                                          left_on = 'item_id', right_index = True, how = 'left'); itemFeaCol.append('Behavior_1'); itemFeatures.columns = itemFeaCol
        itemFeatures = itemFeatures.merge(DataFrame(data[data['behavior_type'] == '2'].pivot_table(values = 'user_id', rows='item_id', aggfunc = len)),
                                          left_on = 'item_id', right_index = True, how = 'left'); itemFeaCol.append('Behavior_2'); itemFeatures.columns = itemFeaCol
        itemFeatures = itemFeatures.merge(DataFrame(data[data['behavior_type'] == '3'].pivot_table(values = 'user_id', rows='item_id', aggfunc = len)),
                                          left_on = 'item_id', right_index = True, how = 'left'); itemFeaCol.append('Behavior_3'); itemFeatures.columns = itemFeaCol
        itemFeatures = itemFeatures.merge(DataFrame(data[data['behavior_type'] == '4'].pivot_table(values = 'user_id', rows='item_id', aggfunc = len)),
                                          left_on = 'item_id', right_index = True, how = 'left'); itemFeaCol.append('Behavior_4'); itemFeatures.columns = itemFeaCol
        itemFeatures = itemFeatures.merge(DataFrame(data[((data['HH'] >= 18) & (data['HH'] <= 24))].pivot_table(values = 'user_id', rows='item_id', aggfunc = len)),
                                          left_on = 'item_id', right_index = True, how = 'left'); itemFeaCol.append('records_evening'); itemFeatures.columns = itemFeaCol
        itemFeatures = itemFeatures.merge(DataFrame(data[((data['HH'] >= 0) & (data['HH'] <= 7))].pivot_table(values = 'user_id', rows='item_id', aggfunc = len)),
                                          left_on = 'item_id', right_index = True, how = 'left'); itemFeaCol.append('records_midnight'); itemFeatures.columns = itemFeaCol
        itemFeatures = itemFeatures.merge(DataFrame(data[((data['HH'] >= 13) & (data['HH'] <= 17))].pivot_table(values = 'user_id', rows='item_id', aggfunc = len)),
                                          left_on = 'item_id', right_index = True, how = 'left'); itemFeaCol.append('records_afternoon'); itemFeatures.columns = itemFeaCol
        itemFeatures = itemFeatures.merge(DataFrame(data[((data['HH'] >= 8) & (data['HH'] <= 12))].pivot_table(values = 'user_id', rows='item_id', aggfunc = len)),
                                          left_on = 'item_id', right_index = True, how = 'left'); itemFeaCol.append('records_morning'); itemFeatures.columns = itemFeaCol   
                            
        print "Time for item feature exaction: %.3f" % (time.time() - START_TIME)                                    

        ## features of category
        categoryFeaCol = ['item_category']
        categoryFeatures = DataFrame(data['item_category'].unique(), columns = categoryFeaCol)
        categoryFeatures = categoryFeatures.merge(DataFrame(data.pivot_table(values = 'user_id', rows='item_category', aggfunc = lambda x:len(x.unique()))),
                                          left_on = 'item_category', right_index = True); categoryFeaCol.append('userCount'); categoryFeatures.columns = categoryFeaCol
        categoryFeatures = categoryFeatures.merge(DataFrame(data.pivot_table(values = 'user_id', rows='item_category', aggfunc = len)),
                                          left_on = 'item_category', right_index = True); categoryFeaCol.append('Records'); categoryFeatures.columns = categoryFeaCol
        categoryFeatures = categoryFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='item_category', aggfunc = lambda x:max(x) - min(x) + 1)),
                                          left_on = 'item_category', right_index = True); categoryFeaCol.append('Period'); categoryFeatures.columns = categoryFeaCol
        categoryFeatures = categoryFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='item_category', aggfunc = lambda x:len(x.unique()))),
                                          left_on = 'item_category', right_index = True); categoryFeaCol.append('ClickDays'); categoryFeatures.columns = categoryFeaCol        
        categoryFeatures = categoryFeatures.merge(DataFrame(data[data['behavior_type'] == '1'].pivot_table(values = 'user_id', rows='item_category', aggfunc = len)),
                                          left_on = 'item_category', right_index = True, how = 'left'); categoryFeaCol.append('Behavior_1'); categoryFeatures.columns = categoryFeaCol
        categoryFeatures = categoryFeatures.merge(DataFrame(data[data['behavior_type'] == '2'].pivot_table(values = 'user_id', rows='item_category', aggfunc = len)),
                                          left_on = 'item_category', right_index = True, how = 'left'); categoryFeaCol.append('Behavior_2'); categoryFeatures.columns = categoryFeaCol
        categoryFeatures = categoryFeatures.merge(DataFrame(data[data['behavior_type'] == '3'].pivot_table(values = 'user_id', rows='item_category', aggfunc = len)),
                                          left_on = 'item_category', right_index = True, how = 'left'); categoryFeaCol.append('Behavior_3'); categoryFeatures.columns = categoryFeaCol
        categoryFeatures = categoryFeatures.merge(DataFrame(data[data['behavior_type'] == '4'].pivot_table(values = 'user_id', rows='item_category', aggfunc = len)),
                                          left_on = 'item_category', right_index = True, how = 'left'); categoryFeaCol.append('Behavior_4'); categoryFeatures.columns = categoryFeaCol
        categoryFeatures = categoryFeatures.merge(DataFrame(data[((data['HH'] >= 18) & (data['HH'] <= 24))].pivot_table(values = 'user_id', rows='item_category', aggfunc = len)),
                                          left_on = 'item_category', right_index = True, how = 'left'); categoryFeaCol.append('records_evening'); categoryFeatures.columns = categoryFeaCol
        categoryFeatures = categoryFeatures.merge(DataFrame(data[((data['HH'] >= 0) & (data['HH'] <= 7))].pivot_table(values = 'user_id', rows='item_category', aggfunc = len)),
                                          left_on = 'item_category', right_index = True, how = 'left'); categoryFeaCol.append('records_midnight'); categoryFeatures.columns = categoryFeaCol
        categoryFeatures = categoryFeatures.merge(DataFrame(data[((data['HH'] >= 13) & (data['HH'] <= 17))].pivot_table(values = 'user_id', rows='item_category', aggfunc = len)),
                                          left_on = 'item_category', right_index = True, how = 'left'); categoryFeaCol.append('records_afternoon'); categoryFeatures.columns = categoryFeaCol
        categoryFeatures = categoryFeatures.merge(DataFrame(data[((data['HH'] >= 8) & (data['HH'] <= 12))].pivot_table(values = 'user_id', rows='item_category', aggfunc = len)),
                                          left_on = 'item_category', right_index = True, how = 'left'); categoryFeaCol.append('records_morning'); categoryFeatures.columns = categoryFeaCol   
 
        print "Time for category feature exaction: %.3f" % (time.time() - START_TIME)                                    

        ## features of userItem
        userItemFeaCol = ['user_item_pairs']
        userItemFeatures = DataFrame(data['user_item_pairs'].unique(), columns = userItemFeaCol)
        userItemFeatures = userItemFeatures.merge(DataFrame(data.pivot_table(values = 'user_id', rows='user_item_pairs', aggfunc = lambda x:len(x.unique()))),
                                          left_on = 'user_item_pairs', right_index = True); userItemFeaCol.append('userCount'); userItemFeatures.columns = userItemFeaCol
        userItemFeatures = userItemFeatures.merge(DataFrame(data.pivot_table(values = 'user_id', rows='user_item_pairs', aggfunc = len)),
                                          left_on = 'user_item_pairs', right_index = True); userItemFeaCol.append('Records'); userItemFeatures.columns = userItemFeaCol
        userItemFeatures = userItemFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='user_item_pairs', aggfunc = lambda x:max(x) - min(x) + 1)),
                                          left_on = 'user_item_pairs', right_index = True); userItemFeaCol.append('Period'); userItemFeatures.columns = userItemFeaCol
        userItemFeatures = userItemFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='user_item_pairs', aggfunc = lambda x:len(x.unique()))),
                                          left_on = 'user_item_pairs', right_index = True); userItemFeaCol.append('ClickDays'); userItemFeatures.columns = userItemFeaCol        
        userItemFeatures = userItemFeatures.merge(DataFrame(data[data['behavior_type'] == '1'].pivot_table(values = 'user_id', rows='user_item_pairs', aggfunc = len)),
                                          left_on = 'user_item_pairs', right_index = True, how = 'left'); userItemFeaCol.append('Behavior_1'); userItemFeatures.columns = userItemFeaCol
        userItemFeatures = userItemFeatures.merge(DataFrame(data[data['behavior_type'] == '2'].pivot_table(values = 'user_id', rows='user_item_pairs', aggfunc = len)),
                                          left_on = 'user_item_pairs', right_index = True, how = 'left'); userItemFeaCol.append('Behavior_2'); userItemFeatures.columns = userItemFeaCol
        userItemFeatures = userItemFeatures.merge(DataFrame(data[data['behavior_type'] == '3'].pivot_table(values = 'user_id', rows='user_item_pairs', aggfunc = len)),
                                          left_on = 'user_item_pairs', right_index = True, how = 'left'); userItemFeaCol.append('Behavior_3'); userItemFeatures.columns = userItemFeaCol
        userItemFeatures = userItemFeatures.merge(DataFrame(data[data['behavior_type'] == '4'].pivot_table(values = 'user_id', rows='user_item_pairs', aggfunc = len)),
                                          left_on = 'user_item_pairs', right_index = True, how = 'left'); userItemFeaCol.append('Behavior_4'); userItemFeatures.columns = userItemFeaCol
        userItemFeatures = userItemFeatures.merge(DataFrame(data[((data['HH'] >= 18) & (data['HH'] <= 24))].pivot_table(values = 'user_id', rows='user_item_pairs', aggfunc = len)),
                                          left_on = 'user_item_pairs', right_index = True, how = 'left'); userItemFeaCol.append('records_evening'); userItemFeatures.columns = userItemFeaCol
        userItemFeatures = userItemFeatures.merge(DataFrame(data[((data['HH'] >= 0) & (data['HH'] <= 7))].pivot_table(values = 'user_id', rows='user_item_pairs', aggfunc = len)),
                                          left_on = 'user_item_pairs', right_index = True, how = 'left'); userItemFeaCol.append('records_midnight'); userItemFeatures.columns = userItemFeaCol
        userItemFeatures = userItemFeatures.merge(DataFrame(data[((data['HH'] >= 13) & (data['HH'] <= 17))].pivot_table(values = 'user_id', rows='user_item_pairs', aggfunc = len)),
                                          left_on = 'user_item_pairs', right_index = True, how = 'left'); userItemFeaCol.append('records_afternoon'); userItemFeatures.columns = userItemFeaCol
        userItemFeatures = userItemFeatures.merge(DataFrame(data[((data['HH'] >= 8) & (data['HH'] <= 12))].pivot_table(values = 'user_id', rows='user_item_pairs', aggfunc = len)),
                                          left_on = 'user_item_pairs', right_index = True, how = 'left'); userItemFeaCol.append('records_morning'); userItemFeatures.columns = userItemFeaCol   

        print "Time for user-item feature exaction: %.3f" % (time.time() - START_TIME)                                    

        ## features of userItem
        userCategoryFeaCol = ['user_category_pairs']
        userCategoryFeatures = DataFrame(data['user_category_pairs'].unique(), columns = userCategoryFeaCol)
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data.pivot_table(values = 'user_id', rows='user_category_pairs', aggfunc = lambda x:len(x.unique()))),
                                          left_on = 'user_category_pairs', right_index = True); userCategoryFeaCol.append('userCount'); userCategoryFeatures.columns = userCategoryFeaCol
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data.pivot_table(values = 'user_id', rows='user_category_pairs', aggfunc = len)),
                                          left_on = 'user_category_pairs', right_index = True); userCategoryFeaCol.append('Records'); userCategoryFeatures.columns = userCategoryFeaCol
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='user_category_pairs', aggfunc = lambda x:max(x) - min(x) + 1)),
                                          left_on = 'user_category_pairs', right_index = True); userCategoryFeaCol.append('Period'); userCategoryFeatures.columns = userCategoryFeaCol
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='user_category_pairs', aggfunc = lambda x:len(x.unique()))),
                                          left_on = 'user_category_pairs', right_index = True); userCategoryFeaCol.append('ClickDays'); userCategoryFeatures.columns = userCategoryFeaCol        
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data[data['behavior_type'] == '1'].pivot_table(values = 'user_id', rows='user_category_pairs', aggfunc = len)),
                                          left_on = 'user_category_pairs', right_index = True, how = 'left'); userCategoryFeaCol.append('Behavior_1'); userCategoryFeatures.columns = userCategoryFeaCol
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data[data['behavior_type'] == '2'].pivot_table(values = 'user_id', rows='user_category_pairs', aggfunc = len)),
                                          left_on = 'user_category_pairs', right_index = True, how = 'left'); userCategoryFeaCol.append('Behavior_2'); userCategoryFeatures.columns = userCategoryFeaCol
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data[data['behavior_type'] == '3'].pivot_table(values = 'user_id', rows='user_category_pairs', aggfunc = len)),
                                          left_on = 'user_category_pairs', right_index = True, how = 'left'); userCategoryFeaCol.append('Behavior_3'); userCategoryFeatures.columns = userCategoryFeaCol
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data[data['behavior_type'] == '4'].pivot_table(values = 'user_id', rows='user_category_pairs', aggfunc = len)),
                                          left_on = 'user_category_pairs', right_index = True, how = 'left'); userCategoryFeaCol.append('Behavior_4'); userCategoryFeatures.columns = userCategoryFeaCol
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data[((data['HH'] >= 18) & (data['HH'] <= 24))].pivot_table(values = 'user_id', rows='user_category_pairs', aggfunc = len)),
                                          left_on = 'user_category_pairs', right_index = True, how = 'left'); userCategoryFeaCol.append('records_evening'); userCategoryFeatures.columns = userCategoryFeaCol
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data[((data['HH'] >= 0) & (data['HH'] <= 7))].pivot_table(values = 'user_id', rows='user_category_pairs', aggfunc = len)),
                                          left_on = 'user_category_pairs', right_index = True, how = 'left'); userCategoryFeaCol.append('records_midnight'); userCategoryFeatures.columns = userCategoryFeaCol
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data[((data['HH'] >= 13) & (data['HH'] <= 17))].pivot_table(values = 'user_id', rows='user_category_pairs', aggfunc = len)),
                                          left_on = 'user_category_pairs', right_index = True, how = 'left'); userCategoryFeaCol.append('records_afternoon'); userCategoryFeatures.columns = userCategoryFeaCol
        userCategoryFeatures = userCategoryFeatures.merge(DataFrame(data[((data['HH'] >= 8) & (data['HH'] <= 12))].pivot_table(values = 'user_id', rows='user_category_pairs', aggfunc = len)),
                                          left_on = 'user_category_pairs', right_index = True, how = 'left'); userCategoryFeaCol.append('records_morning'); userCategoryFeatures.columns = userCategoryFeaCol   

        print "Time for user-category feature exaction: %.3f" % (time.time() - START_TIME)                                                                   
        
        ## fill na by 0
        userFeatures = userFeatures.fillna(0)
        itemFeatures = itemFeatures.fillna(0)
        categoryFeatures = categoryFeatures.fillna(0)
        userItemFeatures = userItemFeatures.fillna(0)
        userCategoryFeatures = userCategoryFeatures.fillna(0)
        ## calculate features
        userFeatures         = user_feature_cal(userFeatures)     
        itemFeatures         = item_feature_cal(itemFeatures)
        categoryFeatures     = category_feature_cal(categoryFeatures)
        userItemFeatures     = userItemFeatureCal(userItemFeatures)
        userCategoryFeatures = userCategoryFeatureCal(userCategoryFeatures)
        
        print "Time for calculating features: %.3f" % (time.time() - START_TIME)                                    

        ## dump pickle
        dump_pickle(userFeatures, "pickle\\userFeatures.pickle")
        dump_pickle(itemFeatures, "pickle\\itemFeatures.pickle")    
        dump_pickle(categoryFeatures, "pickle\\categoryFeatures.pickle")
        dump_pickle(userItemFeatures, "pickle\\userItemFeatures.pickle")    
        dump_pickle(userCategoryFeatures, "pickle\\userCategoryFeatures.pickle")     

        print "Time for pickle dump: %.3f" % (time.time() - START_TIME)                                    
 
    return userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures

def user_feature_cal(userFeatures):
    
    userFeatures['LogDayFreq'] = userFeatures['Online_Days'] * 1.0 / userFeatures['Period'] 
    userFeatures['ViewPerDay'] = userFeatures['records'] * 1.0 / userFeatures['Online_Days'] 
    userFeatures['MorningProp'] = userFeatures['records_morning'] * 1.0 / userFeatures['records']        
    userFeatures['AfternoonProp'] = userFeatures['records_afternoon'] * 1.0 / userFeatures['records']   
    userFeatures['EveningProp'] = userFeatures['records_evening'] * 1.0 / userFeatures['records']        
    userFeatures['MidnightProp'] = userFeatures['records_midnight'] * 1.0 / userFeatures['records']          
    userFeatures['ViewProp'] = userFeatures['Behavior_1'] * 1.0 / userFeatures['records']            
    userFeatures['FavorProp'] = userFeatures['Behavior_2'] * 1.0 / userFeatures['records']            
    userFeatures['CartProp'] = userFeatures['Behavior_3'] * 1.0 / userFeatures['records']            
    userFeatures['PayProp'] = userFeatures['Behavior_4'] * 1.0 / userFeatures['records']       
    userFeatures['PayViewRate'] = userFeatures['Behavior_4'] * 1.0 / userFeatures['Behavior_1']        
    userFeatures['PayFavorRate'] = userFeatures['Behavior_4'] * 1.0 / userFeatures['Behavior_2']    
    userFeatures['PayCartRate'] = userFeatures['Behavior_4'] * 1.0 / userFeatures['Behavior_3']        
    userFeatures['FavorViewRate'] = userFeatures['Behavior_2'] * 1.0 / userFeatures['Behavior_1']       
    userFeatures['CartViewRate'] = userFeatures['Behavior_3'] * 1.0 / userFeatures['Behavior_1']      
    userFeatures['CartFavorRate'] = userFeatures['Behavior_3'] * 1.0 / userFeatures['Behavior_2']     
 
    userFeatures = userFeatures.fillna(0)
    userFeatures = userFeatures.replace('inf',-999)
    
    return userFeatures

def item_feature_cal(itemFeatures):
    
    itemFeatures['ViewPerClickDay'] = itemFeatures['Records'] * 1.0 / itemFeatures['ClickDays'] 
    itemFeatures['PayPerUser'] = itemFeatures['Behavior_4'] * 1.0 / itemFeatures['userCount'] 
    itemFeatures['ViewPerUser'] = itemFeatures['Behavior_1'] * 1.0 / itemFeatures['userCount'] 
    itemFeatures['CartPerUser'] = itemFeatures['Behavior_3'] * 1.0 / itemFeatures['userCount'] 
    itemFeatures['FavorPerUser'] = itemFeatures['Behavior_2'] * 1.0 / itemFeatures['userCount'] 
    itemFeatures['MorningProp'] = itemFeatures['records_morning'] * 1.0 / itemFeatures['Records']        
    itemFeatures['AfternoonProp'] = itemFeatures['records_afternoon'] * 1.0 / itemFeatures['Records']   
    itemFeatures['EveningProp'] = itemFeatures['records_evening'] * 1.0 / itemFeatures['Records']        
    itemFeatures['MidnightProp'] = itemFeatures['records_midnight'] * 1.0 / itemFeatures['Records']         
    itemFeatures['ViewProp'] = itemFeatures['Behavior_1'] * 1.0 / itemFeatures['Records']            
    itemFeatures['FavorProp'] = itemFeatures['Behavior_2'] * 1.0 / itemFeatures['Records']            
    itemFeatures['CartProp'] = itemFeatures['Behavior_3'] * 1.0 / itemFeatures['Records']            
    itemFeatures['PayProp'] = itemFeatures['Behavior_4'] * 1.0 / itemFeatures['Records']       
    itemFeatures['PayViewRate'] = itemFeatures['Behavior_4'] * 1.0 / itemFeatures['Behavior_1']        
    itemFeatures['PayFavorRate'] = itemFeatures['Behavior_4'] * 1.0 / itemFeatures['Behavior_2']      
    itemFeatures['PayCartRate'] = itemFeatures['Behavior_4'] * 1.0 / itemFeatures['Behavior_3']        
    itemFeatures['FavorViewRate'] = itemFeatures['Behavior_2'] * 1.0 / itemFeatures['Behavior_1']       
    itemFeatures['CartViewRate'] = itemFeatures['Behavior_3'] * 1.0 / itemFeatures['Behavior_1']      
    itemFeatures['CartFavorRate'] = itemFeatures['Behavior_3'] * 1.0 / itemFeatures['Behavior_2']     
 
    itemFeatures = itemFeatures.fillna(0)
    itemFeatures = itemFeatures.replace('inf',-999)  
    
    return itemFeatures

def category_feature_cal(categoryFeatures):
    
    categoryFeatures['ViewPerClickDay'] = categoryFeatures['Records'] * 1.0 / categoryFeatures['ClickDays'] 
    categoryFeatures['ClickPerUser'] = categoryFeatures['Records'] * 1.0 / categoryFeatures['userCount'] 
    categoryFeatures['PayPerUser'] = categoryFeatures['Behavior_4'] * 1.0 / categoryFeatures['userCount'] 
    categoryFeatures['ViewPerUser'] = categoryFeatures['Behavior_1'] * 1.0 / categoryFeatures['userCount'] 
    categoryFeatures['CartPerUser'] = categoryFeatures['Behavior_3'] * 1.0 / categoryFeatures['userCount'] 
    categoryFeatures['FavorPerUser'] = categoryFeatures['Behavior_2'] * 1.0 / categoryFeatures['userCount'] 
    categoryFeatures['MorningProp'] = categoryFeatures['records_morning'] * 1.0 / categoryFeatures['Records']        
    categoryFeatures['AfternoonProp'] = categoryFeatures['records_afternoon'] * 1.0 / categoryFeatures['Records']   
    categoryFeatures['EveningProp'] = categoryFeatures['records_evening'] * 1.0 / categoryFeatures['Records']        
    categoryFeatures['MidnightProp'] = categoryFeatures['records_midnight'] * 1.0 / categoryFeatures['Records']         
    categoryFeatures['ViewProp'] = categoryFeatures['Behavior_1'] * 1.0 / categoryFeatures['Records']            
    categoryFeatures['FavorProp'] = categoryFeatures['Behavior_2'] * 1.0 / categoryFeatures['Records']            
    categoryFeatures['CartProp'] = categoryFeatures['Behavior_3'] * 1.0 / categoryFeatures['Records']            
    categoryFeatures['PayProp'] = categoryFeatures['Behavior_4'] * 1.0 / categoryFeatures['Records']       
    categoryFeatures['PayViewRate'] = categoryFeatures['Behavior_4'] * 1.0 / categoryFeatures['Behavior_1']        
    categoryFeatures['PayFavorRate'] = categoryFeatures['Behavior_4'] * 1.0 / categoryFeatures['Behavior_2']      
    categoryFeatures['PayCartRate'] = categoryFeatures['Behavior_4'] * 1.0 / categoryFeatures['Behavior_3']        
    categoryFeatures['FavorViewRate'] = categoryFeatures['Behavior_2'] * 1.0 / categoryFeatures['Behavior_1']       
    categoryFeatures['CartViewRate'] = categoryFeatures['Behavior_3'] * 1.0 / categoryFeatures['Behavior_1']      
    categoryFeatures['CartFavorRate'] = categoryFeatures['Behavior_3'] * 1.0 / categoryFeatures['Behavior_2']     
 
    categoryFeatures = categoryFeatures.fillna(0)
    categoryFeatures = categoryFeatures.replace('inf',-999)  

    return categoryFeatures

def userItemFeatureCal(userItemFeatures):
    
    userItemFeatures['ViewPerOnlineDay'] = userItemFeatures['Records'] * 1.0 / userItemFeatures['ClickDays'] 
    userItemFeatures['MorningProp'] = userItemFeatures['records_morning'] * 1.0 / userItemFeatures['Records']        
    userItemFeatures['AfternoonProp'] = userItemFeatures['records_afternoon'] * 1.0 / userItemFeatures['Records']   
    userItemFeatures['EveningProp'] = userItemFeatures['records_evening'] * 1.0 / userItemFeatures['Records']        
    userItemFeatures['MidnightProp'] = userItemFeatures['records_midnight'] * 1.0 / userItemFeatures['Records']          
    userItemFeatures['ViewProp'] = userItemFeatures['Behavior_1'] * 1.0 / userItemFeatures['Records']            
    userItemFeatures['FavorProp'] = userItemFeatures['Behavior_2'] * 1.0 / userItemFeatures['Records']            
    userItemFeatures['CartProp'] = userItemFeatures['Behavior_3'] * 1.0 / userItemFeatures['Records']            
    userItemFeatures['PayProp'] = userItemFeatures['Behavior_4'] * 1.0 / userItemFeatures['Records']       
    
    userItemFeatures = userItemFeatures.fillna(0)
    userItemFeatures = userItemFeatures.replace('inf', -999)

    return userItemFeatures

def userCategoryFeatureCal(userCategoryFeatures):
    
    userCategoryFeatures['ViewPerOnlineDay'] = userCategoryFeatures['Records'] * 1.0 / userCategoryFeatures['ClickDays'] 
    userCategoryFeatures['MorningProp'] = userCategoryFeatures['records_morning'] * 1.0 / userCategoryFeatures['Records']        
    userCategoryFeatures['AfternoonProp'] = userCategoryFeatures['records_afternoon'] * 1.0 / userCategoryFeatures['Records']   
    userCategoryFeatures['EveningProp'] = userCategoryFeatures['records_evening'] * 1.0 / userCategoryFeatures['Records']        
    userCategoryFeatures['MidnightProp'] = userCategoryFeatures['records_midnight'] * 1.0 / userCategoryFeatures['Records']          
    userCategoryFeatures['ViewProp'] = userCategoryFeatures['Behavior_1'] * 1.0 / userCategoryFeatures['Records']            
    userCategoryFeatures['FavorProp'] = userCategoryFeatures['Behavior_2'] * 1.0 / userCategoryFeatures['Records']            
    userCategoryFeatures['CartProp'] = userCategoryFeatures['Behavior_3'] * 1.0 / userCategoryFeatures['Records']            
    userCategoryFeatures['PayProp'] = userCategoryFeatures['Behavior_4'] * 1.0 / userCategoryFeatures['Records']       
    
    userCategoryFeatures = userCategoryFeatures.fillna(0)
    userCategoryFeatures = userCategoryFeatures.replace('inf', -999)

    return userCategoryFeatures

## combine exacted features with train data
def featureCombination(train_user, userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures):
  
    header = train_user.columns

    train_user = train_user.merge(userFeatures, left_on = 'user_id', right_on = 'user_id', how = 'left')
    header = header.append(userFeatures.columns[1::] + '_u'); train_user.columns = header
    train_user = train_user.merge(itemFeatures, left_on = 'item_id', right_on = 'item_id', how = 'left')
    header = header.append(itemFeatures.columns[1::] + '_i'); train_user.columns = header
    train_user = train_user.merge(categoryFeatures, left_on = 'item_category', right_on = 'item_category', how = 'left')
    header = header.append(categoryFeatures.columns[1::] + '_c'); train_user.columns = header   
    train_user = train_user.merge(userItemFeatures, left_on = 'user_item_pairs', right_on = 'user_item_pairs', how = 'left')
    header = header.append(userItemFeatures.columns[1::] + '_u&i'); train_user.columns = header   
    train_user = train_user.merge(userCategoryFeatures, left_on = 'user_category_pairs', right_on = 'user_category_pairs', how = 'left')
    header = header.append(userCategoryFeatures.columns[1::] + '_u&c'); train_user.columns = header      
    
    return train_user


if __name__ == '__main__':

    LOAD_FROM_PICKLE = True
#   train_item, header_item, train_user, header_user = readData(itemSize = 1000000, userSize = 10000000)   
    train_item, header_item, train_user, header_user = readSampleData()
    train_user, header_user = columnProcess(train_user)
    train_user = listToDataFrame(train_user, header_user)
    
    userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures =feature_exaction(train_user, LOAD_FROM_PICKLE)


    
    
    
    
    
    
    