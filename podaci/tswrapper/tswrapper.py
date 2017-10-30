# -*- coding: utf-8 -*-
"""
Created on Fri Oct 20 13:49:13 2017

@author: ldh
"""

# tushare.py

import tushare as ts

def get_universe(symbol):
    '''
    获取**当前**全A、指定板块、指数、ST的成分股代码。
    
    Parameters
    -----------
        symbol 获取类型
    Returns
    ----------
        list [ticker,...]
    Notes
    ---------
    'A' 
        全A股
    'st' 
        st股票
    'hs300' 
        沪深300成分股
    'cyb' 
        创业板成分股
    'sz50' 
        上证50成分股
    'A-st' 
        剔除st股票后的全A股
    
    '''
    if symbol == 'A':
        return ts.get_stock_basics().index.values.tolist()
    if symbol == 'st':
        return ts.get_st_classified()['code'].values.tolist()
    if symbol == 'hs300':
        return ts.get_hs300s()['code'].values.tolist()
    if symbol == 'cyb':
        return ts.get_gem_classified()['code'].values.tolist()
    if symbol == 'sz50':
        return ts.get_sz50s()['code'].values.tolist()
    if symbol == 'A-st':
        A = set(ts.get_stock_basics().index.values.tolist())
        ST = set(ts.get_st_classified()['code'].values.tolist())
        for st in ST:
            A.discard(st)        
        return list(A)


if __name__ == '__main__':
    universe = get_universe('sz50')
