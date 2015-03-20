#coding=utf-8

from data_import import readData,readSampleData

def train_item_process(data, header):

    pass










if __name__ == '__main__':
    train_item, header_item, train_user, header_user = readSampleData()
#   train_item, header_item, train_user, header_user = readData(itemSize = 1000000, userSize = 10000000)
    train_item_process(train_item, header_item)
    
    print header_item
    print train_item[0]