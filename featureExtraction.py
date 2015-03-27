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
        DAYS = (int(timeSplit[0])-2014)*365 + monthDaysDict[timeSplit[1]] + int(timeSplit2[0]) - 304 - 17 ##count from 20141118      
        data[i].append(DAYS);                   data[i].append(DAYS + int(timeSplit2[1]) * 1.0/24)
        ##add user_item pair and user_category pair
        data[i].append((data[i][0],data[i][1]))
        data[i].append((data[i][0],data[i][4]))
        data[i].append((data[i][1],data[i][4]))
        
    header = ['user_id','item_id','behavior_type','user_geohash','item_category','time','YYYY','MM',
              'DD','HH','Days','D&H','user_item_pairs','user_category_pairs','item_category_pairs']
        
    return data, header
## features of user
def user_feature_process(data):
    userFeaCol   = ['user_id']
    userFeatures = DataFrame(data['user_id'].unique(), columns = userFeaCol)
    userFeatures = userFeatures.merge(DataFrame(data.pivot_table(values = 'item_id', rows='user_id', aggfunc = lambda x:len(x.unique()))),
                                      left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('itemCount')
    userFeatures = userFeatures.merge(DataFrame(data.pivot_table(values = 'item_id', rows='user_id', aggfunc = len)),
                                      left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('records')
    userFeatures = userFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='user_id', aggfunc = lambda x:len(x.unique()))),
                                      left_on = 'user_id', right_index = True, how = 'left'); userFeaCol.append('Online_Days')
    userFeatures = userFeatures.merge(DataFrame(data.pivot_table(values = 'Days', rows='user_id', aggfunc = lambda x: (max(x) - min(x) + 1))),
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
    userFeatures = userFeatures.fillna(0)
    
    return userFeatures
## features of category
def category_feature_process(data):
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
    categoryFeatures = categoryFeatures.fillna(0) 
    
    return categoryFeatures

## features of item
def item_feature_process(data):
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
    itemFeatures.fillna(0)   
    
    return itemFeatures

## features of category
def userCategory_feature_process(data):
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
    userCategoryFeatures = userCategoryFeatures.fillna(0)
    
    return userCategoryFeatures



def columnProcess_pd(data):
    data['D&H']  = data['time'].apply(func = lambda x: float(x.split(' ')[0].split('-')[2]) + float(x.split(' ')[1])/24)
    print data['D&H']
    data['user_category_pairs'] = data['user_id'].apply(func = lambda x: str(x) +',') +data['category_id'].apply(func = lambda x: str(x))
    return data


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
def featureCombination(train_user, userFeatures, categoryFeatures,  userCategoryFeatures):
  
    header = train_user.columns

    train_user = train_user.merge(userFeatures, left_on = 'user_id', right_on = 'user_id', how = 'left')
    header = header.append(userFeatures.columns[1::] + '_u'); train_user.columns = header
#    train_user = train_user.merge(itemFeatures, left_on = 'item_id', right_on = 'item_id', how = 'left')
#    header = header.append(itemFeatures.columns[1::] + '_i'); train_user.columns = header
    train_user = train_user.merge(categoryFeatures, left_on = 'item_category', right_on = 'item_category', how = 'left')
    header = header.append(categoryFeatures.columns[1::] + '_c'); train_user.columns = header   
   
    train_user = train_user.merge(userCategoryFeatures, left_on = 'user_category_pairs', right_on = 'user_category_pairs', how = 'left')
    header = header.append(userCategoryFeatures.columns[1::] + '_u&c'); train_user.columns = header      
    
    return train_user

def data_preprocess_1(train_user, Days):
    header = train_user.columns.tolist()
    train_user = train_user.merge(DataFrame(train_user.pivot_table(values = 'D&H', rows = 'user_category_pairs', aggfunc = 'max')),
                                  left_on = 'user_category_pairs', right_index = True, how = 'left')
    header.append('U&C_lastRecordTime'); train_user.columns = header
    
    train_user['Day_interval'] = train_user['U&C_lastRecordTime'].apply(func=lambda x:int(x)) - train_user['Days'].apply(func=lambda x:int(x))
    header.append('Day_interval')
    
    train_record_c1 = train_user[(train_user['U&C_lastRecordTime'] == train_user['D&H']) & (train_user['Days'] >= (31 - Days))]
    train_record_c1 = train_record_c1.merge(DataFrame(train_record_c1[train_record_c1['behavior_type'] == '4'].pivot_table(values = 'D&H', rows = 'user_category_pairs', aggfunc = lambda x: 1 if len(x) > 0 else 0)),
                                            left_on = 'user_category_pairs', right_index = True, how = 'left'); header.append('if_pay'); train_record_c1.columns = header            
    ##features of c1_last15
    c1_last15 = train_user[((train_user['Day_interval'] <= 15) & (train_user['Day_interval'] > 0))]
    c1_last15_fea = DataFrame(c1_last15.user_category_pairs.unique(), columns = ['user_category_pairs']); header_c1_last15_fea = ['user_category_pairs']
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[c1_last15['behavior_type'] == '1'].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('viewCount_L15'); c1_last15_fea.columns = header_c1_last15_fea
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[c1_last15['behavior_type'] == '2'].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('favorCount_L15'); c1_last15_fea.columns = header_c1_last15_fea                                       
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[c1_last15['behavior_type'] == '3'].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('cartCount_L15'); c1_last15_fea.columns = header_c1_last15_fea        
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[c1_last15['behavior_type'] == '4'].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('payCount_L15'); c1_last15_fea.columns = header_c1_last15_fea                   
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[c1_last15['behavior_type'] == '1'].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = 'min')),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('lastViewInterval'); c1_last15_fea.columns = header_c1_last15_fea                   
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[c1_last15['behavior_type'] == '2'].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = 'min')),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('lastFavorInterval'); c1_last15_fea.columns = header_c1_last15_fea                    
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[c1_last15['behavior_type'] == '3'].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = 'min')),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('lastCartInterval'); c1_last15_fea.columns = header_c1_last15_fea                   
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[c1_last15['behavior_type'] == '4'].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = 'min')),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('lastPayInterval'); c1_last15_fea.columns = header_c1_last15_fea                   
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  7) & (c1_last15['behavior_type'] == '1')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('viewCount_L7'); c1_last15_fea.columns = header_c1_last15_fea
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  7) & (c1_last15['behavior_type'] == '2')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('favorCount_L7'); c1_last15_fea.columns = header_c1_last15_fea                                       
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  7) & (c1_last15['behavior_type'] == '3')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('cartCount_L7'); c1_last15_fea.columns = header_c1_last15_fea        
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  7) & (c1_last15['behavior_type'] == '4')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('payCount_L7'); c1_last15_fea.columns = header_c1_last15_fea                   
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  3) & (c1_last15['behavior_type'] == '1')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('viewCount_L3'); c1_last15_fea.columns = header_c1_last15_fea
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  3) & (c1_last15['behavior_type'] == '2')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('favorCount_L3'); c1_last15_fea.columns = header_c1_last15_fea                                       
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  3) & (c1_last15['behavior_type'] == '3')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('cartCount_L3'); c1_last15_fea.columns = header_c1_last15_fea        
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  3) & (c1_last15['behavior_type'] == '4')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('payCount_L3'); c1_last15_fea.columns = header_c1_last15_fea                   
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  1) & (c1_last15['behavior_type'] == '1')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('viewCount_L1'); c1_last15_fea.columns = header_c1_last15_fea
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  1) & (c1_last15['behavior_type'] == '2')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('favorCount_L1'); c1_last15_fea.columns = header_c1_last15_fea                                       
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  1) & (c1_last15['behavior_type'] == '3')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('cartCount_L1'); c1_last15_fea.columns = header_c1_last15_fea        
    c1_last15_fea = c1_last15_fea.merge(DataFrame(c1_last15[(c1_last15['Day_interval']  <=  1) & (c1_last15['behavior_type'] == '4')].pivot_table(values = 'Day_interval',rows = 'user_category_pairs', aggfunc = len)),
                                        left_on = 'user_category_pairs', right_index = True, how = 'left'); header_c1_last15_fea.append('payCount_L1'); c1_last15_fea.columns = header_c1_last15_fea                            
                                        
    # generate train set
    train_record_c1 = train_record_c1.drop_duplicates(cols = 'user_category_pairs', take_last = True)
    train_record_c1 = train_record_c1.merge(c1_last15_fea, on = 'user_category_pairs', how = 'left')
    train_record_c1 = train_record_c1.fillna(0)
    del train_record_c1['Day_interval']
    
    return train_record_c1
