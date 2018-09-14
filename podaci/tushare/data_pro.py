# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 13:53:46 2018

@author: ldh

进阶的tushare数据接口
1. 首次获取tushare数据从网络请求,然后保存本地
2. 再次请求,会从本地读取数据
3. 提供本地数据更新功能
"""

# data_pro.py
import datetime as dt
import os
import yaml
import pandas as pd
import tushare as ts

# 载入配置
etc_path = os.path.join(os.path.split(__file__)[0],'etc.yaml')
with open(etc_path,'r') as f:
    etc = yaml.load(f)
tushare_token = etc['tushare_token']
data_pro_path = etc['data_pro_path']

items_path = os.path.join(os.path.split(__file__)[0],'items.txt')
with open(items_path,'r') as f:
    items = f.readline().split(',')


    
class DataPro():
    def __init__(self):
        ts.set_token(tushare_token)
        self.pro = ts.pro_api()
        self.local_items = items
        
    def update_data(self):
        '''
        更新本地数据.
        '''
        # 股票列表
        if 'stock_basic' in self.local_items:
            stock_basic = self.pro.query('stock_basic',fileds = 'ts_code,symbol,name,list_date,delist_date,list_status')
            stock_basic.to_hdf(os.path.join(data_pro_path,'stock_basic.h5'),mode = 'w',append = True)
        if 'trade_cal' in self.local_items:
            trade_cal = self.pro.query('trade_cal',start_date = '19910101',
                                       end_date = (dt.date.today() + dt.timedelta(days = 365)).strftime('%Y%m%d'))
            trade_cal.to_hdf(os.path.join(data_pro_path,'trade_cal.h5'),'trade_cal',mode = 'w',append = True)
            
    def _add_item(self,item_name):
        with open(items_path,'r') as f:
            content_len = len(f.readline())
        if content_len == 0:            
            with open(items_path,'a') as f:
                f.write('%s'%item_name)
        else:
            with open(items_path,'a') as f:
                f.write(',%s'%item_name)
        self.local_items.append(item_name)

    def get_stock_universe(self):
        '''
        获取股票列表。
        '''
        if 'stock_basic' in self.local_items:
            return pd.read_hdf(os.path.join(data_pro_path,'stock_basic.h5'),'stock_basic')
        else:
            stock_basic = self.pro.query('stock_basic',
                                         fileds = 'ts_code,symbol,name,list_date,delist_date,list_status')
            stock_basic.to_hdf(os.path.join(data_pro_path,'stock_basic.h5'),mode = 'w',append = True)
            self._add_item('stock_basic')
            return stock_basic
        
    def get_trade_calendar(self):
        if 'trade_cal' in self.local_items:
            return pd.read_hdf(os.path.join(data_pro_path,'trade_cal.h5'),'trade_cal')
        else:
            trade_cal = self.pro.query('trade_cal',start_date = '19910101',
                                       end_date = (dt.date.today() + dt.timedelta(days = 365)).strftime('%Y%m%d'))
            trade_cal.to_hdf(os.path.join(data_pro_path,'trade_cal.h5'),'trade_cal',mode = 'w',append = True)
            self._add_item('trade_cal')
            return trade_cal
        
if __name__ == '__main__':
    dp = DataPro()
    