# -*- coding: utf-8 -*-
"""
Created on Thu Aug 30 17:07:14 2018

@author: ldh
"""

# data.py

import time
import os
import pandas as pd
import datetime as dt
import yaml
import tushare as ts
from utils import data_file_name_convertor

etc_path = os.path.join(os.path.split(__file__)[0],'etc.yaml')

with open(etc_path,'r') as f:
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
        if 'data.h5' not in os.listdir(stock_data_path):
            start_date = '19910101'
            end_date = dt.datetime.today().strftime('%Y%m%d')    
        else:
            start_date = dt.datetime.strptime(pd.read_hdf(save_path,key = 'last_update_dt')[0].iloc[-1],
                                              '%Y%m%d') + dt.timedelta(days = 1)
            start_date = start_date.strftime('%Y%m%d')
            end_date = dt.datetime.today().strftime('%Y%m%d')
            
            if end_date < start_date:
                print 'Company %s Data has already been the latest!'%stock_code
                return
        
    for df_name in df_names:
        if df_name == 'balancesheet':
            data_dict[df_name + '_merge'] = pro.query(df_name,ts_code = stock_code,
                     start_date = start_date,
                     end_date = end_date,
                     report_type = 1).drop_duplicates(subset = ['ann_date','end_date']) 
        else:
            data_dict[df_name + '_merge'] = pro.query(df_name,ts_code = stock_code,
                     start_date = start_date,
                     end_date = end_date,
                     report_type = 1).drop_duplicates(subset = ['ann_date','end_date'])             
            data_dict[df_name + '_single_quarter'] = pro.query(df_name,ts_code = stock_code,
                     start_date = start_date,
                     end_date = end_date,
                     report_type = 2).drop_duplicates(subset = ['ann_date','end_date'])  
        
    new_data_flag = 0
    
    for key,val in data_dict.items():
        if len(val) != 0:
            new_data_flag = 1
            break
        
    if new_data_flag == 0:
        print 'No new data'
        return
    
    last_update_dt = pd.DataFrame([[end_date]])
    
    # 更改数据编码
    col_names = ['ts_code','ann_date','f_ann_date','end_date',
                 'comp_type','report_type']
    
    for key,val in data_dict.items():
        if len(val) != 0:
            for col in col_names:
                val[col] = val[col].apply(lambda x:x.encode('utf8'))
            
    for key,val in data_dict.items():
        val.to_hdf(save_path,key = key,append = True)
    last_update_dt.to_hdf(save_path,'last_update_dt',append = True)        
    
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

def symbol_convertor(row):
    if row['exchange_id'] == 'SSE':
        return row['symbol'] + '.SH'
    elif row['exchange_id'] == 'SZSE':
        return row['symbol'] + '.SZ'
    
def refresh_data(stock_list):
    '''
    更新名单中的股票数据。
    '''

    total_time = 0
    for stk in stock_list:
        start_time = time.time()
        refresh_company_data(stk)
        end_time = time.time()
        time_cost = end_time - start_time
        total_time += time_cost
        print '%s has been refreshed sucessfully which costs %s , the total time cost is %s'%(stk,time_cost,total_time)
        
if __name__ == '__main__':
#    refresh_company_data('000860.SZ')
#    data = get_company_data('000860.SZ')
    stock_basic = get_stock_universe()
    stock_list = stock_basic['stock_code'].tolist() 
    while True:
        try:
            refresh_data(stock_list[:1000])
            print 'All Data Over'
            break
        except Exception as e:
            print e
            time.sleep(10)
            continue
#    da = get_company_data('000860.SZ')