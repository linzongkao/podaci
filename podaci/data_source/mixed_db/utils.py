# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 13:39:20 2017

@author: ldh
"""

# utils.py
import numpy as np

def func_1(df):
    '''
    对数据进行前复权处理。
    '''

    df_sorted = df.sort_values('trade_date',ascending = False)
    latest_recover_gene = df_sorted['recover_gene'].values[0]
    df_sorted['open_price'] = df_sorted['open_price'] / latest_recover_gene
    df_sorted['high_price'] = df_sorted['high_price'] / latest_recover_gene
    df_sorted['low_price'] = df_sorted['low_price'] / latest_recover_gene
    df_sorted['close_price'] = df_sorted['close_price'] / latest_recover_gene
    return df_sorted

def func_2(num_str):
    '''
    转换类型将str转成int
    '''
    return int(num_str)
func_2 = np.frompyfunc(func_2,1,1)
