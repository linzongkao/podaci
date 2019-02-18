# -*- coding: utf-8 -*-
"""
Created on Wed Feb 13 10:23:56 2019

@author: ldh
"""

# api.py

from __future__ import print_function
from database_engine import DatabaseEngine

engine_guosen_obj = DatabaseEngine('guosen')
engine_guosen = engine_guosen_obj.get_engine()
session_guosen = engine_guosen_obj.get_session()

def index_daily(index_code,start_date,end_date):
    '''
    指数日行情。
    
    Parameters
    ----------
    index_code
        指数代码
    start_date
        开始日期
    end_date
        结束日期
        
    Returns
    --------
    DataFrame
    '''
    ##[TODO]数据不齐全，只有从2016年2月开始的数据?
    sql = '''   SELECT b.TradingCode,*
    from IX_IndexQuote a 
    LEFT JOIN SecuMain b ON b.SecuID = a.SecuID
    WHERE b.TradingCode = '000001'
    order by TradingDate ASC'''
    pass

