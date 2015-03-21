#coding=utf-8
import pandas as pd
import numpy as np
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
        data[i].append((int(timeSplit[0])-2014)*365 + monthDaysDict[timeSplit[1]] + int(timeSplit2[0]))    
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
    itemCategoryDict = {}

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
        if not itemCategoryDict.has_key(data.ix[i,'item_category_pairs']):
            userItemDict[data.ix[i,'item_category_pairs']] = {}        
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
    dump_pickle(itemCategoryDict, "pickle\\itemCategoryDict.pickle")      
    
    userDict, itemDict, categoryDict, userItemDict, userCategoryDict, itemCategoryDict = load_pickle() 
      
    return userDict, itemDict, categoryDict, userItemDict, userCategoryDict, itemCategoryDict
       
    '''   
    if LOAD_FROM_PICKLE:
        userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures, itemCategoryFeatures = load_pickle()
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

        print "Time for user features exaction: %.3f" % (time.time() - START_TIME)

        ## features of item
        itemFeaCol = ['item_id']
        itemFeatures = DataFrame(data['item_id'].unique(), columns = itemFeaCol)
        itemFeatures = itemFeatures.merge(DataFrame(data.pivot_table(values = 'user_id', rows='item_id', aggfunc = lambda x:len(x.unique()))),
                                          left_on = 'item_id', right_index = True); itemFeaCol.append('userCount'); itemFeatures.columns = itemFeaCol
        itemFeatures = itemFeatures.merge(DataFrame(data.pivot_table(values = 'user_id', rows='item_id', aggfunc = len)),
                                          left_on = 'item_id', right_index = True); itemFeaCol.append('itemRecord'); itemFeatures.columns = itemFeaCol
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
                            
        print "Time for item features exaction: %.3f" % (time.time() - START_TIME)                                    

        ## features of category
        categoryFeaCol = ['item_category']
        categoryFeatures = DataFrame(data['item_category'].unique(), columns = categoryFeaCol)
        







        print "Time for category features exaction: %.3f" % (time.time() - START_TIME)                                    

        ## features of userItem
        userItemFeaCol = ['user_item_pairs']
        userItemFeatures = DataFrame(data['user_item_pairs'].unique(), columns = userItemFeaCol)









        print "Time for user user-item exaction: %.3f" % (time.time() - START_TIME)                                    

        ## features of userItem
        userCategoryFeaCol = ['user_category_pairs']
        userCategoryFeatures = DataFrame(data['user_category_pairs'].unique(), columns = userCategoryFeaCol)









        print "Time for user user-category exaction: %.3f" % (time.time() - START_TIME)                                    

        ## features of itemCategory
        itemCategoryFeaCol = ['item_category_pairs']
        itemCategoryFeatures = DataFrame(data['item_category_pairs'].unique(), columns = itemCategoryFeaCol)







        print "Time for user item-category exaction: %.3f" % (time.time() - START_TIME)                                    

    
#        '''
        dump_pickle(userFeatures, "pickle\\userFeatures.pickle")
        dump_pickle(itemFeatures, "pickle\\itemFeatures.pickle")    
        dump_pickle(categoryFeatures, "pickle\\categoryFeatures.pickle")
        dump_pickle(userItemFeatures, "pickle\\userItemFeatures.pickle")    
        dump_pickle(userCategoryFeatures, "pickle\\userCategoryFeatures.pickle")    
        dump_pickle(itemCategoryFeatures, "pickle\\itemCategoryFeatures.pickle")  
#        '''
        print "Time for pickle dump: %.3f" % (time.time() - START_TIME)                                    

 
    return userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures, itemCategoryFeatures


    

if __name__ == '__main__':

    LOAD_FROM_PICKLE = False
    
    train_item, header_item, train_user, header_user = readSampleData()
#   train_item, header_item, train_user, header_user = readData(itemSize = 1000000, userSize = 10000000)
    train_user, header_user = columnProcess(train_user)
    train_user = listToDataFrame(train_user, header_user)
    
    userFeatures, itemFeatures, categoryFeatures, userItemFeatures, userCategoryFeatures, itemCategoryFeatures =feature_exaction(train_user, LOAD_FROM_PICKLE)

    print train_user[0:2]
    print userFeatures.ix[0:2,:]
    print itemFeatures.ix[0:2,:]
    #print pd.groupby(train_user['user_id'], by = train_user['user_id']).agg(len)