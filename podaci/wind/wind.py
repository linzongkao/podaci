# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 15:53:38 2017

@author: ldh
"""

# wind.py
import pandas as pd
from WindPy import *
from utils import wind_symbol_2_code,code_2_wind_symbol

w.start()
def get_stocks_factors(universe,factors,trade_date):
    '''
    获取多个股票在某一日的多个因子数据。
    
    Parameters
    ----------
    universe
        list ['600340','000001']
    factors
        'pe_ttm,pb_mrq'
    trade_date
        '20171011'
        
    Returns
    --------
    '''
    universe_wind = code_2_wind_symbol(universe)
    universe_wind = ','.join(universe_wind)
    data = w.wss(universe_wind,factors,'tradeDate=%s'%trade_date)
    df = pd.DataFrame(data.Data,index = data.Fields,columns = universe).T
    return df
    
def get_stocks_industries(universe):
    '''
    获取多个股票对应的万德行业。
    '''
    pass

def get_stocks_factors_with_industry(universe,factors,trade_date):
    '''
    获取多个股票
    '''
    pass

if __name__ == '__main__':
    universe = ['600340','000001']
    factors = 'pe_ttm,pb_mrq'
    trade_date = '20171011'
    data = get_stocks_factors(universe,factors,trade_date)
