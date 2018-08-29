# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 13:07:06 2018

@author: ldh
"""

# data.py
import datetime as dt
import pandas as pd
from database_engine import DatabaseEngine
from SQLs import (SQL_GET_SW_INDEX_CLOSE,
                  SQL_GET_FUND_HOLD_STOCK,
                  SQL_GET_SW_INDEX_STOCK_COMPONENTS,
                  SQL_GET_FUND_INFER_INDUSTRY,
                  SQL_GET_FUND_BASIC,
                  SQL_GET_FUND_SCORE,
                  SQL_GET_FUND_NET_VALUE,
                  SQL_GET_TRADE_CALENDAR,
                  SQL_GET_FUND_INFER_INDUSTRY_CURRENT,
                  SQL_GET_FUNDS_NET_VALUE)
from consts import SW_INDUSTRY_FIRST_CODE

engine_ld_obj = DatabaseEngine('ld')
engine_ld = engine_ld_obj.get_engine()

engine_xiaoyi_obj = DatabaseEngine('xiaoyi')
engine_xiaoyi = engine_xiaoyi_obj.get_engine()
session_xiaoyi = engine_xiaoyi_obj.get_session()

engine_wind_obj = DatabaseEngine('wind')
engine_wind = engine_wind_obj.get_engine()

def get_sw_industry_index(start_date,end_date):
    '''
    获取申万一级行业指数收盘值。
    '''
    codes = ["\'" + each + "\'" for each in SW_INDUSTRY_FIRST_CODE ]
    codes = ",".join(codes)
    data = pd.read_sql(SQL_GET_SW_INDEX_CLOSE.format(start_date = start_date,
                                                     end_date = end_date,
                                                     codes = codes),engine_ld)
    return data

def get_fund_hold_stock_top10(end_date):
    '''
    获取截止日期前全市场基金最新持仓。
    
    Parameters
    ------------
    end_date
        YYYYMMDD,截止日期
        
    Notes
    --------
    加入了end_date_adj来控制披露信息尽量接近end_date,并且提高查询速度
    '''
    end_date_adj = dt.datetime.strptime(end_date,'%Y%m%d') - dt.timedelta(days = 200)
    end_date_adj = end_date_adj.strftime('%Y%m%d')
    
    data = pd.read_sql(SQL_GET_FUND_HOLD_STOCK.format(end_date = end_date,
                                                      end_date_adj = end_date_adj),
        engine_xiaoyi)
    return data

def get_stock_sw_industry(target_date):
    '''
    获取目标日期申万一级成分.
    
    Parameters
    ----------
    target_date
        YYYYMMDD
    '''
    from consts import SW_INDUSTRY_FIRST_CODE,SW_INDUSTRY_FIRST_NAME
    ser = pd.Series(SW_INDUSTRY_FIRST_CODE,index = SW_INDUSTRY_FIRST_NAME)
    ser.name = 'level_1_code'
    data = pd.read_sql(SQL_GET_SW_INDEX_STOCK_COMPONENTS.format(target_date = target_date),
                       engine_ld)
    data_adj = data.join(ser,on = 'level_1')
    return data_adj

def get_fund_sw_infer(target_date):
    '''
    获取目标日期基金推断申万行业比例
    
    Parameters
    ----------
    target_date
        YYYYMMDD
    '''
    return pd.read_sql(SQL_GET_FUND_INFER_INDUSTRY%target_date,engine_xiaoyi)

def get_fund_sw_infer_current():
    '''
    获取最新基金推断申万行业比例
    '''
    return pd.read_sql(SQL_GET_FUND_INFER_INDUSTRY_CURRENT,engine_xiaoyi)
    
def get_fund_basic_info(fund_type_list):
    '''
    获取基金基本信息.
    
    Parameters
    -----------
    fund_type_list
        list of str 基金类型
        股票型,混合型,债券型,货币市场型
    '''
    fund_types = ["investment_type = '%s'"%each for each in fund_type_list]
    sql = SQL_GET_FUND_BASIC + ' or '.join(fund_types)
    return pd.read_sql(sql,engine_xiaoyi)

def get_fund_score():
    '''
    获取基金评分.
    '''
    return pd.read_sql(SQL_GET_FUND_SCORE,engine_xiaoyi)


def get_fund_net_value(trade_code,start_date,end_date):
    '''
    获取基金净值.
    
    Parameters
    ----------
    trade_code
        基金代码
    start_date
        str
    end_date
        str
    '''
    return pd.read_sql(SQL_GET_FUND_NET_VALUE.format(trade_code = trade_code,
                                                     start_date = start_date,
                                                     end_date = end_date),
        engine_xiaoyi)
    
def get_funds_net_value(funds_universe,start_date,end_date):
    '''
    获取多只基金净值。
    '''
    funds_universe = ["'%s'"%each for each in funds_universe]
    funds_universe = ",".join(funds_universe)
    return pd.read_sql(SQL_GET_FUNDS_NET_VALUE.format(funds_universe = funds_universe,
                                                      start_date = start_date,
                                                      end_date = end_date),
        engine_xiaoyi)
        
def get_trade_calendar(start_date = '20170101',
                       end_date = dt.date.today().strftime('%Y%m%d')):
    '''
    获取交易日历。
    '''
    return pd.read_sql(SQL_GET_TRADE_CALENDAR.format(start_date = start_date,
                                                     end_date = end_date),
    engine_ld)
    


if __name__ == '__main__':
#    data = get_sw_industry_index('20180801','20180820')
#    data = get_fund_hold_stock_top10('20180801')
#    data = get_stock_sw_industry('20180801')
#    data = get_fund_sw_infer('20180822')
#    data = get_fund_basic_info(['股票型'])
#    data = get_fund_score()
#    data = get_fund_net_value('210013','20180101','20180201')
#    data = get_trade_calendar()
    data = get_funds_net_value([u'530003', u'200010', u'002152', u'960028', u'000294'],
                               start_date,end_date)