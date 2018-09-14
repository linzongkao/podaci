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
import os
import yaml
import tushare as ts

etc_path = os.path.join(os.path.split(__file__)[0],'etc.yaml')
items_path = os.path.join(os.path.split(__file__)[0],'items.txt')
with open(etc_path,'r') as f:
    etc = yaml.load(f)
tushare_token = etc['tushare_token']

class DataItem():
    def __init__(self,name):
        self.name = name

class DataPro():
    def __init__(self):
        ts.set_token(tushare_token)
        self.data_items = {}
        self.pro = ts.pro_api()
        self._load_items()
        
    def _load_items():
        '''
        读取已有的数据项。
        '''
        pass
        
    def update_data(self):
        '''
        更新本地数据.
        '''
        pass
    
    def get_stock_universe(self):
        '''
        获取股票列表。
        '''
        
    
    