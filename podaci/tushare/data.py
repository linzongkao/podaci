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
from utils import data_file_name_convertor,if_company_data_exists

etc_path = os.path.join(os.path.split(__file__)[0],'etc.yaml')

with open(etc_path,'r') as f:
    etc = yaml.load(f)
    
company_data_path = etc['company_data_path']
trade_data_path = etc['trade_data_path']
tushare_token = etc['tushare_token']

ts.set_token(tushare_token)
pro = ts.pro_api()



def refresh_company_data(stock_code):
    '''
    刷新公司数据.
    包括: 公司单季度三大财务报表;公司最新合并财务报表,首次更新于20180901
    使用注意事项: 未来公司调整过去财务报表
    '''
    df_names = ['income','balancesheet','cashflow']
    data_dict = {}
    stock_data_path = os.path.join(company_data_path,data_file_name_convertor(stock_code))
    save_path = os.path.join(stock_data_path,'data.h5')
    
    if not if_company_data_exists(stock_code,company_data_path):
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
#                try:
                val[col] = val[col].fillna(method = 'pad')
                val[col] = val[col].apply(lambda x:x.encode('utf8'))
#                except:
#                    print 'stock_code:%s,table_name:%s,col_name:%s'%(stock_code,key,col)
            
    for key,val in data_dict.items():
        val.to_hdf(save_path,key = key,append = True)
    last_update_dt.to_hdf(save_path,'last_update_dt',append = True)        
    
    print 'Company %s Data GET SUCCESSFULLY'%stock_code
    
def refresh_trade_data1(stock_code):
    '''
    刷新公司交易数据。
    包括
    1. 日线行情
    2. 每日指标
    '''
    df_names1 = ['daily','daily_basic'] # 增量更新
    df_names2 = ['adj_factor','suspend'] # 重新刷新
    data_dict = {}
    stock_data_path = os.path.join(trade_data_path,data_file_name_convertor(stock_code))
    save_path = os.path.join(stock_data_path,'data.h5')
    
    if not if_company_data_exists(stock_code,trade_data_path):
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
        
    for df_name in df_names1:
        data_dict[df_name] = pro.query(df_name,ts_code = stock_code,
                     start_date = start_date,
                     end_date = end_date)
        data_dict[df_name +'_last_update_dt'] = data_dict[df_name]['trade_date'].max()
        
    for df_name in df_names2:
        data_dict[df_name] = pro.query(df_name,ts_code = stock_code,
                 trade_date = '')
    
    # 更改数据编码
    col_names = ['ts_code','trade_date']
    
    for key,val in data_dict.items():
        if len(val) != 0:
            for col in col_names:
#                try:
                val[col] = val[col].fillna(method = 'pad')
                val[col] = val[col].apply(lambda x:x.encode('utf8'))
#                except:
#                    print 'stock_code:%s,table_name:%s,col_name:%s'%(stock_code,key,col)
            
    for key,val in data_dict.items():
        val.to_hdf(save_path,key = key,append = True)
    last_update_dt.to_hdf(save_path,'last_update_dt',append = True)        
    
    print 'Company %s Data GET SUCCESSFULLY'%stock_code
    
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
        
def refresh_all():
    '''
    更新所有数据。
    '''
    pass

if __name__ == '__main__':
    refresh_company_data('600072.SH')
#    data = get_company_data('000860.SZ')
#    stock_basic = get_stock_universe()
#    stock_list = stock_basic['stock_code'].tolist() 
#    while True:
#        try:
#            refresh_data(stock_list[3420:])
#            print 'All Data Over'
#            break
#        except Exception as e:
#            print e
#            time.sleep(10)
#            continue
#    da = get_company_data('001965.SZ')
#    os.system('shutdown -s -t 30')