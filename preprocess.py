#coding=utf-8

import pandas as pd
from pandas.core.frame import DataFrame

def data_preprocess_1(train_user):
    header = train_user.columns.tolist()
    train_user = train_user.merge(DataFrame(train_user.pivot_table(values = 'D&H', rows = 'user_category_pairs', aggfunc = 'max')),
                                  left_on = 'user_category_pairs', right_index = True, how = 'left')
    header.append('U&C_lastRecordTime'); train_user.columns = header
    
    train_user['Day_interval'] = train_user['U&C_lastRecordTime'].apply(func=lambda x:int(x)) - train_user['Days']
    train_record = train_user[((train_user['U&C_lastRecordTime'] == train_user['D&H']) | (train_user['behavior_type'] == '4'))]
    
    
    #buy and continue to view , 
    #buy and stop viewing
    
    
    train_user.to_csv("pickle\\tmp.csv")
    return train_user
