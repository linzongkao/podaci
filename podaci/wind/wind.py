# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 15:53:38 2017

@author: ldh
"""

# wind.py
import pandas as pd
from WindPy import *
from utils import (wind_symbol_2_code,code_2_wind_symbol,
                   dict_2_str,date_format_convert)

w.start()
def get_wss(universe,factors,if_convert = False,**options):
    '''
    获取万德多维数据。
    
    Parameters
    ----------
    universe
        list ['600340','000001']
    factors
        'pe_ttm,pb_mrq'
    trade_date
        '20171011'
    if_convert
        是否将universe转换成wind代码,默认为False,仅支持沪深股票
    options
        其他参数
        
    Returns
    --------
    DataFrame    
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
    data = get_wss(universe,factors_,True,**options)
    index_codes = list(set(data['INDEXCODE_WIND'].dropna().values)) # 避免bug
    index_data = get_wss(index_codes,factors,False,**options)
    columns = index_data.columns
    industry_columns = [each + '_industry' for each in columns]
    index_data.columns = pd.Index(industry_columns)
    data = data.join(index_data,on = 'INDEXCODE_WIND')
    return data

def get_wsd(universe,factors,start_date,end_date,if_convert = False,**options):
    '''
    获取万德日期序列数据。
    
    Parameters
    ----------
    universe
        list ['600340','000001']
    factors
        'pe_ttm,pb_mrq'
    start_date
        '20171011'
    end_date
        '20171021'
    if_convert
        是否将universe转换成wind代码,默认为False,仅支持沪深股票
    options
        其他参数
        
    Returns
    --------
    DataFrame
    
    Notes
    ----------
    universe与factors最多有一个是多维。
    '''
    options = dict_2_str(options)
    start_date = date_format_convert(start_date)
    end_date = date_format_convert(end_date)
    
    if if_convert:
        universe_wind = code_2_wind_symbol(universe)
        universe_wind = ','.join(universe_wind)
    else:
        universe_wind = ','.join(universe)
    
    data = w.wsd(universe_wind,factors,start_date,end_date,options)

    if len(universe) == 1:
        df = pd.DataFrame(data.Data,columns = data.Times,index = data.Fields).T
    else:
        df = pd.DataFrame(data.Data,columns = data.Times,index = data.Codes).T
    return df

if __name__ == '__main__':
    universe = ['000001.SZ','000011.SZ']
    factors = 'dma'
    start_date = '20151011'
    end_date = '20151216'
    data = get_wsd(universe,'dma',start_date,end_date,False,Period = 'M')
    
