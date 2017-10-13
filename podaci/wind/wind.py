# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 15:53:38 2017

@author: ldh
"""

# wind.py
import pandas as pd
from WindPy import *
from utils import wind_symbol_2_code,code_2_wind_symbol,dict_2_str

w.start()
def get_factors(universe,factors,if_convert = True,**options):
    '''
    获取多个因子数据。
    
    Parameters
    ----------
    universe
        list ['600340','000001']
    factors
        'pe_ttm,pb_mrq'
    trade_date
        '20171011'
    if_convert
        是否将universe转换成wind代码,默认为True
    options
        其他参数
        
    Returns
    --------
    '''
    options = dict_2_str(options)
    if if_convert:
        universe_wind = code_2_wind_symbol(universe)
        universe_wind = ','.join(universe_wind)
    else:
        universe_wind = ','.join(universe)
    data = w.wss(universe_wind,factors,options)
    df = pd.DataFrame(data.Data,index = data.Fields,columns = universe).T
    return df
    

def get_stocks_factors_with_industry(universe,factors,**options):
    '''
    获取多个股票因子及对应的行业因子(万德).
    '''
    factors_ = factors + ',indexcode_wind'
    data = get_factors(universe,factors_,True,**options)
    index_codes = list(set(data['INDEXCODE_WIND'].dropna().values)) # 避免bug
    index_data = get_factors(index_codes,factors,False,**options)
    columns = index_data.columns
    industry_columns = [each + '_industry' for each in columns]
    index_data.columns = pd.Index(industry_columns)
    data = data.join(index_data,on = 'INDEXCODE_WIND')
    return data

if __name__ == '__main__':
    universe = ['600340','000001','600000']
    factors = 'pe_ttm'
    trade_date = '20171012'
    data = get_factors(['882007.WI'],factors,False,tradeDate = '20171012', industryType = 2)
    a = get_stocks_factors_with_industry(universe,factors,tradeDate = '20171012',industryType = 1)
