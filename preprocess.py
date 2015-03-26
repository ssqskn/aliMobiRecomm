#coding=utf-8

import pandas as pd
import random
from pandas.core.frame import DataFrame

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


def shuffling(train, target):
    random.shuffle(train)
    random.shuffle(target)
    return train, target