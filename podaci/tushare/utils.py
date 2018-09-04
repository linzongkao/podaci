# -*- coding: utf-8 -*-
"""
Created on Thu Aug 30 19:06:31 2018

@author: ldh
"""

# utils.py

import os

def data_file_name_convertor(stock_code):
    '''
    数据文件名称转换
    '''
    return str(stock_code[:6])

def symbol_convertor(row):
    if row['exchange_id'] == 'SSE':
        return row['symbol'] + '.SH'
    elif row['exchange_id'] == 'SZSE':
        return row['symbol'] + '.SZ'
    
    
def if_company_data_exists(stock_code,data_path):
    '''
    公司数据是否已经on-disk判断。
    '''
    if data_file_name_convertor(stock_code) not in os.listdir(data_path):
        return False
    else:
        return True