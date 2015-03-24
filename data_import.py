#coding=utf-8

import csv
import pandas as pd

def readData_pd():
    PATH = "data\\"
    FILE_ITEM = "tianchi_mobile_recommend_train_item.csv"
    FILE_USER = "tianchi_mobile_recommend_train_user.csv"
    train_user = pd.read_csv(PATH + FILE_USER)
#   train_item = pd.read_csv(PATH + FILE_ITEM)
    train_item = []
    return train_user, train_item

def readData(itemSize, userSize):   ##size indicates how many records to be involved
    PATH = "data\\"
    FILE_ITEM = "tianchi_mobile_recommend_train_item.csv"
    FILE_USER = "tianchi_mobile_recommend_train_user.csv"
    ##read file for item
    f = open(PATH + FILE_ITEM)
    item_obj    = csv.reader(f)
    train_item  = []
    header_item = []
    for i,line in enumerate(item_obj):
        if i == 0:
            header_item.append(line)
        elif (i > 0 and i <= itemSize):
            train_item.append(line)
        else:
            break
    f.close
    ##read file for user
    f = open(PATH + FILE_USER)
    user_obj    = csv.reader(f)
    train_user  = []
    header_user = []
    for i,line in enumerate(user_obj):
        if i == 0:
            header_user.append(line)
        elif (i > 0 and i <= userSize):
            train_user.append(line)
        else:
            break
    f.close

    return train_item, header_item, train_user, header_user


def readSampleData():
    PATH = "data\\"
    FILE_ITEM = "train_item_sample.csv"
    FILE_USER = "train_user_sample.csv"
    ##read file for item
    f = open(PATH + FILE_ITEM)
    item_obj    = csv.reader(f)
    train_item  = []
    header_item = []
    for i,line in enumerate(item_obj):
        if i == 0:
            header_item.append(line)
        else:
            train_item.append(line)
    f.close
    ##read file for user
    f = open(PATH + FILE_USER)
    user_obj    = csv.reader(f)
    train_user  = []
    header_user = []
    for i,line in enumerate(user_obj):
        if i == 0:
            header_user.append(line)
        else:
            train_user.append(line)
    f.close
    
    return train_item, header_item, train_user, header_user
    


if __name__ == '__main__':
    SAVE_SAMPLE_DATA = True
    
    train_item, header_item, train_user, header_user = readData(itemSize = 20000, userSize = 100000)

    if SAVE_SAMPLE_DATA:
        f = open("data\\train_item_sample.csv",'wb')
        writer = csv.writer(f)
        writer.writerow(['item_id','item_geohash','item_category'])
        for line in train_item:
            writer.writerow(line)
        f.close
        
        f = open("data\\train_user_sample.csv",'wb')
        writer = csv.writer(f)
        writer.writerow(['user_id','item_id','behavior_type','user_goehash','item_category','time'])
        for line in train_user:
            writer.writerow(line)
        f.close
    
    print header_item
    print train_item[0:10]
    print header_user
    print train_user[0]
    
