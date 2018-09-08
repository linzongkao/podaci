# -*- coding: utf-8 -*-
"""
Created on Tue Sep 04 09:42:07 2018

@author: ldh
"""

# api.py

import os
import yaml
import pandas as pd
import tushare as ts
from utils import data_file_name_convertor,symbol_convertor

etc_path = os.path.join(os.path.split(__file__)[0],'etc.yaml')
with open(etc_path,'r') as f:
    etc = yaml.load(f)
    
company_data_path = etc['company_data_path']
trade_data_path = etc['trade_data_path']


pro = ts.pro_api()

def get_company_data(stock_code):
    '''
    获取公司数据.
    '''
    stock_data_path = os.path.join(company_data_path,data_file_name_convertor(stock_code))
    save_path = os.path.join(stock_data_path,'data.h5')
    
    df_names = ['income','balancesheet','cashflow']
    data_dict = {}
    
    for df_name in df_names:
        if df_name != 'balancesheet':
            data_dict[df_name + '_merge'] = pd.read_hdf(save_path,key = df_name + '_merge')
            data_dict[df_name + '_single_quarter'] = pd.read_hdf(save_path,key = df_name + '_single_quarter')
        else:
            data_dict[df_name + '_merge'] = pd.read_hdf(save_path,key = df_name + '_merge')
    return data_dict


def get_stock_universe(is_hs ='',list_status = '',exchange_id = ''):
    '''
    获取股票列表.
    
    Pameters
    ---------
    is_hs
        是否沪深港标的
        是否沪深港通标的，N否 H沪股通 S深股通
    list_status
        上市状态
        上市状态 L上市 D退市 P暂停上市
    exchange_id
        交易所id,默认含有全部
        交易所 SSE上交所 SZSE深交所 HKEX港交所
    if_all
        是否包含退市股票,默认不含
    '''
    data = pro.query('stock_basic', exchange_id = exchange_id, 
                     is_hs= is_hs, list_status = list_status,
                     fields='symbol,name,list_date,list_status,delist_date,exchange_id')
    data['stock_code'] = data.apply(symbol_convertor,axis = 1)
    return data

def get_daily(stock_code):
    '''
    获取股票日线行情数据。
    '''
    pass

def get_daily_basic(stock_code):
    '''
    获取股票每日指标.
    '''
    pass

def get_adjfactor(stock_code,trade_date):
    '''
    获取股票复权因子。
    '''
    pass

def get_suspend(stock_code):
    '''
    获取股票停牌数据。
    '''
    pass

