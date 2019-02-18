# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 13:07:06 2018

@author: ldh
"""

# data.py
from __future__ import print_function
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
                  SQL_GET_FUNDS_NET_VALUE,
                  SQL_GET_FUND_MANAGER,
                  SQL_GET_MANAGER_FUND,
                  SQL_GET_STOCK_BASICS,
                  SQL_GET_STOCK_DAILY_DATA1,
                  SQL_GET_STOCK_DAILY_DATA2,
                  SQL_GET_STOCK_MIN_CLOSE,
                  SQL_GET_STOCK_FEATURES,
                  SQL_GET_FUNDS_DAILY_RET,
                  SQL_GET_ALL_FUNDS_DAILY_RET,
                  SQL_GET_INDEX_DAILY,
                  SQL_GET_RISK_FREE_RATE,
                  SQL_GET_STOCK_ADJFACTOR)
from consts import SW_INDUSTRY_FIRST_CODE

engine_ld_obj = DatabaseEngine('ld')
engine_ld = engine_ld_obj.get_engine()

engine_xiaoyi_obj = DatabaseEngine('xiaoyi')
engine_xiaoyi = engine_xiaoyi_obj.get_engine()
session_xiaoyi = engine_xiaoyi_obj.get_session()

engine_wind_obj = DatabaseEngine('wind')
engine_wind = engine_wind_obj.get_engine()

engine_gb_obj = DatabaseEngine('geniusbar')
engine_gb = engine_gb_obj.get_engine()
session_gb = engine_gb_obj.get_session()

engine_xiaoyi40_obj = DatabaseEngine('xiaoyi40')
engine_xiaoyi40 = engine_xiaoyi40_obj.get_engine()
session_xiaoyi40 = engine_xiaoyi40_obj.get_session()

engine_guosen_obj = DatabaseEngine('guosen')
engine_guosen = engine_guosen_obj.get_engine()
session_guosen = engine_guosen_obj.get_session()

#%% 基础
def get_trade_calendar(start_date = '20170101',
                       end_date = dt.date.today().strftime('%Y%m%d')):
    '''
    获取交易日历。
    
    Parameters
    -----------
    start_date
        开始日期,默认20170101
    end_date
        结束日期,默认今日
    '''
    return pd.read_sql(SQL_GET_TRADE_CALENDAR.format(start_date = start_date,
                                                     end_date = end_date),
    engine_ld).drop_duplicates('trade_date')
    
#%% 股票
def get_stock_basic():
    '''
    获取当前上市公司股票基础列表。
    '''
    return pd.read_sql(SQL_GET_STOCK_BASICS,engine_ld)

def get_stock_min_data_close(start_dt,end_dt,stock_universe,year = ''):
    '''
    获取股票分钟线收盘数据。

    Parameters
    -----------
    start_dt
        开始日期
    end_dt
        结束日期
    stock_universe
        list,默认''取全部股票
    year
        分钟线数据年份,''默认取2016年之前的数据,之后的数据需指定年份
    Returns
    --------
    DataFrame
    '''
    stock_universe = ["'%s'"%each for each in stock_universe]
    stock_universe = ",".join(stock_universe)
    return pd.read_sql(SQL_GET_STOCK_MIN_CLOSE.format(year = year,
                                                      start_dt = start_dt,
                                                      end_dt = end_dt,
                                                      stock_universe = stock_universe),
        engine_ld).drop_duplicates(subset = ['stockcode','trade_dt'])
        
def get_stock_min_data_close_multi(start_dt,end_dt,stock_universe,year = ''):
    '''
    获取股票分钟线收盘数据。允许多进程。

    Parameters
    -----------
    start_dt
        开始日期
    end_dt
        结束日期
    stock_universe
        list,默认''取全部股票
    year
        分钟线数据年份,''默认取2016年之前的数据,之后的数据需指定年份
    Returns
    --------
    DataFrame
    '''
    stock_universe = ["'%s'"%each for each in stock_universe]
    stock_universe = ",".join(stock_universe)
    return pd.read_sql(SQL_GET_STOCK_MIN_CLOSE.format(year = year,
                                                      start_dt = start_dt,
                                                      end_dt = end_dt,
                                                      stock_universe = stock_universe),
        DatabaseEngine('ld').get_engine()).drop_duplicates(subset = ['stockcode','trade_dt'])
        
def get_stock_daily_data(start_date,end_date,stock_universe = ''):
    '''
    获取股票日线数据。可获取全部股票或者指定股票的日线数据。
    
    Parameters
    -----------
    start_date
        开始日期
    end_date
        结束日期
    stock_universe
        list,默认''取全部股票  
    
    Returns
    --------
    DataFrame
    '''
    if len(stock_universe) == 0:
        return pd.read_sql(SQL_GET_STOCK_DAILY_DATA1.format(start_date = start_date,
                                                            end_date = end_date),
        engine_ld).drop_duplicates(subset = ['stock_code','trade_date'])
    else:
        stock_universe = ["'%s'"%each for each in stock_universe]
        stock_universe = ",".join(stock_universe)
        return pd.read_sql(SQL_GET_STOCK_DAILY_DATA2.format(start_date = start_date,
                                                            end_date = end_date,
                                                            stock_universe = stock_universe),
        engine_ld).drop_duplicates(subset = ['stock_code','trade_date'])


def get_stock_features(start_date,end_date,stock_universe = ''):
    '''
    获取股票特征数据。
    
    Parameters
    -----------
    start_date
        开始日期
    end_date
        结束日期
    stock_universe
        list,default ''
    '''
    stock_universe = ["'%s'"%each for each in stock_universe]
    stock_universe = ",".join(stock_universe)
    return pd.read_sql(SQL_GET_STOCK_FEATURES.format(start_date = start_date,
                                                     end_date = end_date,
                                                     stock_universe = stock_universe),
        engine_gb)
    
def get_stock_adjfactor(stock_code,start_date,end_date):
    '''
    获取股票复权因子。
    
    Parameters
    -----------
    stock_code
        股票代码
    start_date
        开始日期
    end_date
        结束日期
    '''
    return pd.read_sql(SQL_GET_STOCK_ADJFACTOR.format(stock_code = stock_code,
                                                      start_date = start_date,
                                                      end_date = end_date),
    engine_ld)
    
def get_stock_daily_backward_adj(stock_code,start_date,end_date):
    '''
    获取股票后复权价格。
    
    Parameters
    -----------
    stock_code
        股票代码
    start_date
        开始日期
    end_date
        结束日期
    '''
    stock_daily = get_stock_daily_data(start_date,end_date,stock_universe = [stock_code])
    stock_adjfactor = get_stock_adjfactor(stock_code,start_date,end_date)
    comb = pd.merge(stock_daily,stock_adjfactor,left_on=['trade_date','stock_code'],
                    right_on = ['trade_date','stock_code'])
    comb = comb.sort_values('trade_date',ascending = True).set_index('trade_date')
    comb = comb.drop('stock_code',axis = 1)
    comb.loc[:,'adj_close_price'] = comb['close_price'] * comb['factor']
    return comb
    

#%% 指数
def get_index_daily(index_code,start_date,end_date):
    '''
    获取指数日行情。
    
    Parameters
    -----------
    index_code
        指数代码
    start_date
        开始日期
    end_date
        结束日期
        
    Returns
    -------
    DataFrame
    '''
    data = pd.read_sql(SQL_GET_INDEX_DAILY.format(index_code = index_code,start_date = start_date,end_date = end_date),engine_ld)
    data = data.sort_values('trade_date',ascending = True)
    data = data.drop_duplicates(subset = ['trade_date'])
    return data

def get_sw_industry_index(start_date,end_date):
    '''
    获取申万一级行业指数收盘值。
    '''
    codes = ["\'" + each + "\'" for each in SW_INDUSTRY_FIRST_CODE ]
    codes = ",".join(codes)
    data = pd.read_sql(SQL_GET_SW_INDEX_CLOSE.format(start_date = start_date,
                                                     end_date = end_date,
                                                     codes = codes),engine_ld)
    return data.drop_duplicates(['code','trade_date'])

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

#%% 基金
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
        
def get_fund_manager(fund_universe,trade_date):
    '''
    获取基金对应基金在某日经理列表.
    '''
    fund_universe_adj = ["'%s'"%each for each in fund_universe]
    funds = ','.join(fund_universe_adj)
    return pd.read_sql(SQL_GET_FUND_MANAGER.format(fund_universe = funds,
                                                   trade_date = trade_date),engine_xiaoyi)
def get_manager_fund(managers_ids,trade_date):
    '''
    根据基金经理id获取某日其管理的基金列表。
    
    Parameters
    ------------
    managers_ids
        list
    trade_date
        YYYYMMDD
    '''
    managers_ids = ["'%s'"%each for each in managers_ids]
    managers_ids = ",".join(managers_ids)
    return pd.read_sql(SQL_GET_MANAGER_FUND.format(managers_ids = managers_ids,
                                                   trade_date = trade_date),
        engine_xiaoyi)
        
def get_funds_daily_ret(fund_universe,start_date,end_date):
    '''
    获取基金池基金指定时间区间内的日收益率。
    
    Parameters
    --------------
    fund_universe
        list or None(all)
    start_date
        开始日期,yyyymmdd
    end_date
        结束日期,yyyymmdd
        
    Returns
    -------
    DataFrame
    '''
    if fund_universe is not None:
        universe_str = ','.join(["'%s'"%each for each in fund_universe])
        return pd.read_sql(SQL_GET_FUNDS_DAILY_RET.format(universe_str = universe_str,
                                                          start_date = start_date,
                                                          end_date = end_date),
            engine_xiaoyi)
    else:
        return pd.read_sql(SQL_GET_ALL_FUNDS_DAILY_RET.format(start_date = start_date,
                                                          end_date = end_date),
            engine_xiaoyi)
        
#%% 新闻
def get_news():
    pass

#%% 宏观
def get_risk_free_rate():
    '''
    获取无风险收益率。此处无风险收益率采取个人一年期定期存款利率。
    '''
    return pd.read_sql(SQL_GET_RISK_FREE_RATE,engine_wind)

        
#%% 通用数据获取
def get_data(sql_statement,db_name):
    '''
    通用数据获取接口。
    
    Parameters
    ------------
    sql_statement
        sql语句
    db_name
        写入数据库名,支持gb,xiaoyi    
    '''
    if db_name == 'gb':
        engine = engine_gb
    elif db_name == 'xiaoyi':
        engine = engine_xiaoyi
    elif db_name == 'xiaoyi40':
        engine = engine_xiaoyi40
    elif db_name == 'guosen':
        engine = engine_guosen    
    return pd.read_sql(sql_statement,engine)

#%% 数据写入
def save_into_db(df,table_name,dtype_dict,db_name,if_exists,index = False):
    '''
    写入数据库.
    
    Parameters
    ----------
    df
        需要写入的DataFrame
    table_name
        写入表名
    dtype_dict
        sqlalchemy数据类型dict
    db_name
        写入数据库名,支持gb,xiaoyi,xiaoyi40
    if_exists
        若存在处理方式,'replace'替代,'fail'失败,'append'添加新值
    index
        是否写入index,默认为False
    '''
    if db_name == 'gb':
        df.to_sql(table_name,engine_gb,dtype = dtype_dict,if_exists = if_exists,
                  index = index)
    elif db_name == 'xiaoyi':
        df.to_sql(table_name,engine_xiaoyi,dtype = dtype_dict,if_exists = if_exists,
                  index = index)   
    elif db_name == 'xiaoyi40':
        df.to_sql(table_name,engine_xiaoyi40,dtype = dtype_dict,if_exists = if_exists,
                  index = index)
        
def execute_session(sql_statement,db_name):
    '''
    执行Session操作。
    
    Parameters
    ------------
    sql_statement
        sql语句
    db_name
        写入数据库名,支持gb,xiaoyi,xiaoyi40
    '''
    if db_name == 'gb':
        session_gb.execute(sql_statement)
        session_gb.commit()
    elif db_name == 'xiaoyi':
        session_xiaoyi.execute(sql_statement)
        session_xiaoyi.commit()
    elif db_name == 'xiaoyi40':
        session_xiaoyi40.execute(sql_statement)
        session_xiaoyi40.commit()
    
if __name__ == '__main__':
#    data = get_sw_industry_index('20180801','20180820')
#    data = get_fund_hold_stock_top10('20180801')
#    data = get_stock_sw_industry('20180801')
#    data = get_fund_sw_infer('20180822')
#    data = get_fund_basic_info(['股票型'])
#    data = get_fund_score()
#    data = get_fund_net_value('210013','20180101','20180201')
#    data = get_trade_calendar()
#    data = get_funds_net_value([u'530003', u'200010', u'002152', u'960028', u'000294'],
#                               start_date,end_date)
#    data = get_fund_manager(['519606','001878'],'20171231')
#    data = get_manager_fund(['{AFFBFC95-FA1E-4243-8CF3-D6F7F69B5528}'],'20171231')
#    data = get_stock_basic()
#    data1 = get_stock_daily_data('20180901','20180905',['000860'])
#    data2 = get_stock_min_data_close('20180901','20180905',['000860'],2018)
#    data3 = get_stock_features('20180101','20180201',['000860'])
#    data4 = get_funds_daily_ret(['502056'],'20180101','20181231')
#    data = get_index_daily('000300.SH','20150101','20170101')
#    risk_free = get_risk_free_rate()
#    adj_factor = get_stock_adjfactor('600887','20100101','20181231')
    data = get_stock_daily_backward_adj('600887','20100101','20181231')
    