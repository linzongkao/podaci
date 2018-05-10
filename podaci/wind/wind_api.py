# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 15:53:38 2017

@author: ldh
"""

# wind.py
import pandas as pd
from WindPy import w
from utils import (code_2_wind_symbol,
                   dict_2_str,date_format_convert)
from missfactor.quantlib.data_handle import fund_like

w.start()

def get_wss(universe,factors,if_convert = False,names = None,**options):
    '''
    获取万德多维数据。
    
    Parameters
    ----------
    universe
        list ['600340','000001']
    factors
        'pe_ttm,pb_mrq'
    if_convert
        是否将universe转换成wind代码,默认为False,仅支持沪深股票
    names
        list of str,列别名,默认为None
    options
        其他参数,如tradeDate = '20171009'
        
    Returns
    --------
    DataFrame    
    '''
    options = dict_2_str(options)
    
    if names is not None:
        assert len(names) == len(factors.split(','))
        
    if if_convert:
        universe_wind = code_2_wind_symbol(universe)
        universe_wind = ','.join(universe_wind)
    else:
        universe_wind = ','.join(universe)
    data = w.wss(universe_wind,factors,options)
    
    if names is not None:
        df = pd.DataFrame(data.Data,index = names,columns = universe).T
    else:
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

def get_wsd(universe,factors,start_date,end_date,names = None,
            if_convert = False,**options):
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
    names
        list of str,列别名,默认为None,仅对单标的多因素
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
        if names is not None:
            df = pd.DataFrame(data.Data,columns = data.Times,index = names).T
        else:
            df = pd.DataFrame(data.Data,columns = data.Times,index = data.Fields).T
    else:
        df = pd.DataFrame(data.Data,columns = data.Times,index = data.Codes).T
    return df

def get_wsi(universe,factors,start_datetime,end_datetime,names = None,**options):
    '''
    获取万德分钟序列数据。
    
    Parameters
    ----------
    universe
        list ['600340.SH','000001.SZ']
    factors
        'close,volume'
    start_datetime
        '2017-10-11 09:00:00'
    end_date
        '2017-10-21 15:00:00'
    names
        list of str,列别名,默认为None,仅对单标的多因素
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
    universe_wind = ','.join(universe)
    data = w.wsi(universe_wind,factors,start_datetime,end_datetime,options)

    if len(universe) == 1:
        if names is not None:
            df = pd.DataFrame(data.Data,columns = data.Times,index = names).T
        else:
            df = pd.DataFrame(data.Data,columns = data.Times,index = data.Fields).T
    else:
        df = pd.DataFrame(data.Data,columns = data.Times,index = data.Codes).T
    return df

def get_tdays(start_date,end_date,**options):
    '''
    获取交易日历序列。
    
    Parameters
    ---------------
    start_date
        '20101001'
    end_date
        '20171021'
    options
        其他参数
        
    Returns
    --------
    Series
        Datetime64,ASC
    '''
    options = dict_2_str(options)
    start_date = date_format_convert(start_date)
    end_date = date_format_convert(end_date) 
    
    t_days = w.tdays(start_date, end_date, options)
    t_days = pd.Series(t_days.Data[0])
    return t_days


def get_edb(idx_universe,start_date,end_date,names = None,**options):
    '''
    获取经济数据。
    
    Parameters
    ------------
    idx_universe
        经济指标代码
    start_date
        '20160101'
    end_date
        '20170101'
    names
        list of str,列别名,默认为None
    options
        其他参数
        
    Returns
    --------
    DataFrame
    
    Notes
    ------
    names若不为None,则必须与idx_universe等长对应
    '''
    assert len(names) == len(idx_universe)
    options = dict_2_str(options)
    start_date = date_format_convert(start_date)
    end_date = date_format_convert(end_date) 

    idx_universe = ','.join(idx_universe)
    edb = w.edb(idx_universe,start_date,end_date,options)
    
    df = pd.DataFrame(edb.Data,columns = edb.Times,index = names).T
    return df
    
def get_wsq(universe,if_convert = False):
    '''
    实时行情快照.
    
    Parameters
    -----------
    universe
        list,标的代码
    if_convert
        是否将代码转换成万德格式
        
    Returns
    --------
    Series
        index 万德代码, name 快照时间.
    '''
    if if_convert:
        universe_wind = code_2_wind_symbol(universe)
        universe_wind = ','.join(universe_wind)
    else:
        universe_wind = ','.join(universe)
        
    data = w.wsq(universe_wind,'rt_last')
    data = pd.DataFrame(data.Data,index = data.Times,columns = data.Codes).T
    return data[data.columns[0]]

def get_wset(set_name,**options):
    '''
    获取数据集数据。
    
    Parameters
    -----------
    set_name
        集合名称
    options
        选项
        
    Returns
    ----------
    DataFrame
    
    '''
    options = dict_2_str(options)    
    data = w.wset(set_name,options)
    data = pd.DataFrame(data.Data,index = data.Fields).T
    return data

def prepare_backtest_data(normal_universe,start_date,end_date,
                          output_path,monetary_universe = None):
    '''
    为vectra准备回测数据。数据默认为前复权数据。
    
    Parameters
    -----------
    normal_universe
        list of str,万德编码,可以拿到开搞低收四个价格
    start_date
        '20160101'
    end_date
        '20170101'
    output_path
        str,数据保存地址,含文件名及后缀
    monetary_universe
        list of str,万德编码,货币基金  
        
    Returns
    --------
    DataFrame
    
    Notes
    ------
    货币基金的处理方法: 将货币基金转换成净值为1的基金,最小购买份额为1份。流动性为无穷大。
    '''
    comb_data = pd.DataFrame()
    for sec in normal_universe:
        tmp_data = get_wsd([sec],'open,high,low,close,volume,amt',start_date,
                           end_date,names = ['open_price','high_price',
                                    'low_price','close_price','volume','amount'],
                                             PriceAdj='F')
        tmp_data['trade_code'] = sec
        comb_data = pd.concat([comb_data,tmp_data]) 
        
    if monetary_universe is not None:
        for sec in monetary_universe:
            tmp_data = get_wsd([sec],'mmf_unityield',start_date,end_date,names = ['unit_yield'])
            tmp_data['unit_yield'].iloc[0] += 10000.0
            tmp_data['nv'] = tmp_data['unit_yield'].cumsum()
            tmp = fund_like(tmp_data[['nv']])
            tmp['open_price'] = tmp['nv']
            tmp['high_price'] = tmp['nv']
            tmp['low_price'] = tmp['nv']
            tmp['close_price'] = tmp['nv']
            tmp['amount'] = pd.np.inf
            tmp['volume'] = pd.np.inf
            tmp['trade_code'] = sec
            tmp.drop('nv',axis = 1,inplace = True)
            comb_data = pd.concat([comb_data,tmp])
            
    comb_data.index.name = 'trade_date'
    comb_data.to_excel(output_path)
    return comb_data

if __name__ == '__main__':
    data = get_wsi(['10001025.SH'],'close,volume',
                   '2018-05-10 09:00:00','2018-05-10 14:23:15')
#    data = get_wset('optioncontractbasicinfo',exchange = 'sse',windcode = '510050.SH',
#                    status = 'trading')
#    data = get_wsq(['600340.SH','159924.SZ'])
#    data = prepare_backtest_data(['600340.SH','600001.SH'],'20171201','20180101',
#                                 output_path = 'G:\\Work_ldh\\tmp\\test.xlsx')
#    from podaci.wind.wind_api import get_wsd
#    etf_universe = ['510300.SH','510500.SH','512100.SH',
#                    '518880.SH','513500.SH','513600.SH',
#                    '511010.SH','160609.OF']
#    
#    index_universe = ['000300.SH',
#                     '000905.SH',
#                     '000852.SH',
#                     'AU.SHF',
#                     'SPX.GI',
#                     'HSI.HI',
#                     'H11009.CSI',
#                     'H11025.CSI']
#    
#    normal_universe = etf_universe[:-1]
#    monetary_universe = [etf_universe[-1]]
#    
#    start_date = '20130101'
#    end_date = '20180101'
#    output_path = 'backtest_etf_data.xlsx'
#    bt_data = prepare_backtest_data(normal_universe,start_date,end_date,monetary_universe)