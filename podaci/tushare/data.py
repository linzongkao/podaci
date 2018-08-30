# -*- coding: utf-8 -*-
"""
Created on Thu Aug 30 17:07:14 2018

@author: ldh
"""

# data.py
import os
import pandas as pd
import datetime as dt
import yaml
import tushare as ts
from utils import data_file_name_convertor

with open('etc.yaml','r') as f:
    etc = yaml.load(f)
    
data_path = etc['data_path']
tushare_token = etc['tushare_token']

ts.set_token(tushare_token)
pro = ts.pro_api()



def refresh_company_data(stock_code):
    '''
    刷新公司数据.
    '''
    df_names = ['income','balancesheet','cashflow']
    data_dict = {}
    stock_data_path = os.path.join(data_path,data_file_name_convertor(stock_code))
    save_path = os.path.join(stock_data_path,'data.h5')
    
    if not if_company_data_exists(stock_code):
        # 初次创建数据
        os.mkdir(stock_data_path)
        start_date = '19910101'
        end_date = dt.datetime.today().strftime('%Y%m%d')       
    else:
        # 刷新数据
        start_date = dt.datetime.strptime(pd.read_hdf(save_path,key = 'last_update_dt').values[0][0],
                                          '%Y%m%d') + dt.timedelta(days = 1)
        start_date = start_date.strftime('%Y%m%d')
        end_date = dt.datetime.today().strftime('%Y%m%d')
        
        if end_date < start_date:
            print 'Company %s Data has already been the latest!'%stock_code
            return
        
    for df_name in df_names:
        data_dict[df_name + '_adj'] = pro.query(df_name,ts_code = stock_code,
                 start_date = start_date,end_date = end_date,report_type = 4) 
        data_dict[df_name + '_ori'] = pro.query(df_name,ts_code = stock_code,
                 start_date = start_date,end_date = end_date,report_type = 5) 
    last_update_dt = pd.DataFrame([[end_date]])
    
    for key,val in data_dict.items():
        val.to_hdf(save_path,key = key,mode = 'a')
    last_update_dt.to_hdf(save_path,'last_update_dt',mode = 'a')        
    
    print 'Company %s Data GET SUCCESSFULLY'%stock_code
    
    
def if_company_data_exists(stock_code):
    '''
    公司数据是否已经on-disk判断。
    '''
    if data_file_name_convertor(stock_code) not in os.listdir(data_path):
        return False
    else:
        return True
            
def get_company_data(stock_code):
    '''
    获取公司数据.
    '''
    stock_data_path = os.path.join(data_path,data_file_name_convertor(stock_code))
    save_path = os.path.join(stock_data_path,'data.h5')
    
    df_names = ['income','balancesheet','cashflow']
    data_dict = {}
    
    for df_name in df_names:
        data_dict[df_name + '_adj'] = pd.read_hdf(save_path,key = df_name + '_adj')
        data_dict[df_name + '_ori'] = pd.read_hdf(save_path,key = df_name + '_ori')
    return data_dict

def get_stock_universe(is_hs ='S',list_status = '',exchange_id = ''):
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
    return pro.query('stock_basic', exchange_id = exchange_id, 
                     is_hs= is_hs, list_status = list_status,
                     fields='symbol,name,list_date,list_status,delist_date')
    

if __name__ == '__main__':
    refresh_company_data('000860.SZ')
#    data = get_company_data('000860.SZ')
#    stock_basic = get_stock_universe()
    