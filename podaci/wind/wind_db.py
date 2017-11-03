# -*- coding: utf-8 -*-
"""
Created on Mon Oct 30 16:09:38 2017

@author: ldh

条件:本地有万德DBSync数据库产品。
"""

# wind_db.py

import os
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

settings_path = os.path.dirname(__file__)

with open(os.path.join(settings_path,'etc.yaml'),'r') as f:
    settings = yaml.load(f)

db_engine = create_engine('mssql+pymssql://{user}:{passwd}@{server}/{db}'.format(
        user = settings['user'],passwd = settings['password'],
        server = settings['server'],db = settings['db']))

session_maker = sessionmaker(bind = db_engine)
session = session_maker()

def get_industry_symbols(industry_name = None,industry_level = None):
    '''
    获取最新行业成分。
    
    Parameters
    -----------
    industry_name
        行业名称
    industry_level
        行业级别,1,2,3,4
    '''
    industry_level += 1
    sql = '''
    Select F16_1090 symbol
       ,ob_object_name_1090 name
       ,a.name industry
       From tb_object_1090
     Inner Join tb_object_1400 On F1_1400 = OB_REVISIONS_1090
     Inner Join tb_object_1022 a On substring(f3_1400,1,%s) = substring(a.code,1,%s)
     Where a.code Like '62%%'
      And a.levelnum='%s'
      And F6_1400='1'
      And F4_1090 In ('A','B')
    	AND a.name = '%s'
    '''%(industry_level * 2 , industry_level * 2,industry_level,industry_name)
    df = pd.read_sql(sql,db_engine)
    return df

def get_trade_data(universe,start_date,end_date):
    '''
    获取前复权行情数据.
    '''
    pass
