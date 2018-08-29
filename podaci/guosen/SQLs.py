# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 13:16:09 2018

@author: ldh
"""

# SQLs.py

#%% GET DATA
SQL_GET_SW_INDEX_CLOSE = '''
SELECT CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962),112)  as trade_date
,[stockcode] as code
,[value] as close_price
FROM [BasicData].[dbo].[Yi_IndexDayClose]
WHERE CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962),112) >= '{start_date}'
AND CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962),112) <= '{end_date}'
AND stockcode in ({codes})
order by trade_date 
'''

SQL_GET_FUND_HOLD_STOCK = '''
SELECT c.trade_code,c.name,c.position,c.report_date,c.stock_code,c.stock_name FROM fund_hold_stock_hist c,
(SELECT trade_code,max(report_date) as report_date FROM
(
SELECT DISTINCT trade_code,report_date
FROM [fund_hold_stock_hist]
WHERE report_date <= '{end_date}' AND 
report_date >= '{end_date_adj}' and DATALENGTH(trade_code) = 6) a
GROUP BY a.trade_code) b 
WHERE c.trade_code = b.trade_code AND
c.report_date = b.report_date
'''


#SQL_GET_SW_INDEX_STOCK_COMPONENTS = '''
#SELECT * FROM (
#SELECT a.F16_1090 as stock_code,b.F16_1090 as index_code,c.F3_1475 as include_date,
#(CASE WHEN c.F4_1475 is null THEN '99999999' ELSE c.F4_1475 END) as exclude_date FROM
#TB_OBJECT_1475 c
#LEFT JOIN TB_OBJECT_1090 a ON c.F1_1475=a.F2_1090
#LEFT JOIN TB_OBJECT_1090 b ON c.F2_1475=b.F2_1090) d
#WHERE d.include_date <= '{target_date}'
#AND d.exclude_date > '{target_date}'
#'''

#SQL_GET_SW_INDEX_STOCK_COMPONENTS = '''
#SELECT * FROM (
#SELECT a.F16_1090 as stock_code,b.F16_1090 as index_code,c.F3_1476 as include_date,
#(CASE WHEN c.F4_1476 is null THEN '99999999' ELSE c.F4_1476 END) as exclude_date FROM
#TB_OBJECT_1476 c
#LEFT JOIN TB_OBJECT_1090 a ON c.F1_1476=a.F2_1090
#LEFT JOIN TB_OBJECT_1090 b ON c.F2_1476=b.F2_1090) d
#WHERE d.include_date <= '{target_date}'
#AND d.exclude_date > '{target_date}'
#'''

SQL_GET_SW_INDEX_STOCK_COMPONENTS = '''
SELECT * FROM(
SELECT [股票代码] as stock_code
      ,[板块代码] as bord_code
      ,[一级行业名称] as level_1
      ,[二级行业名称] as level_2
      ,[三级行业名称] as level_3
      ,[纳入日期] as include_date
      ,CASE WHEN [剔除日期] IS NULL THEN '99999999' ELSE [剔除日期] END AS exclude_date
      ,[最新标志] as if_latest
FROM [BasicData].[dbo].[Yi_StockBoardsSW]) d
WHERE d.include_date <= '{target_date}'
AND d.exclude_date > '{target_date}'
'''


SQL_GET_FUND_INFER_INDUSTRY = '''
SELECT [trade_code]
      ,[pct]
      ,[industry_name]
      ,[industry_code]
      ,[update_date]
FROM [sds209635243_db].[dbo].[fund_hold_industry_infer_hist]
WHERE update_date = '%s'
'''

SQL_GET_FUND_INFER_INDUSTRY_CURRENT = '''
SELECT [trade_code]
      ,[pct]
      ,[industry_name]
      ,[industry_code]
      ,[update_date]
FROM [sds209635243_db].[dbo].[fund_hold_industry_infer]
'''

SQL_GET_FUND_BASIC = '''
SELECT 
[name]
,[trade_code]
,[investment_type]
,[if_index]
FROM [sds209635243_db].[dbo].[fund_basic]
WHERE 
'''


SQL_GET_FUND_SCORE = '''
SELECT 
trade_code,
score,
investment_type
FROM fund_score
'''

SQL_GET_FUND_NET_VALUE = '''
SELECT 
[trade_code]
,[trade_date]
,[net_value]
,[net_value_adj]
FROM [sds209635243_db].[dbo].[fund_net_value]
WHERE trade_code = '{trade_code}'
AND trade_date >= '{start_date}'
AND trade_date <= '{end_date}'
'''

SQL_GET_FUNDS_NET_VALUE = '''
SELECT 
[trade_code]
,[trade_date]
,[net_value]
,[net_value_adj]
FROM [sds209635243_db].[dbo].[fund_net_value]
WHERE trade_code IN ({funds_universe})
AND trade_date >= '{start_date}'
AND trade_date <= '{end_date}'
'''

SQL_GET_TRADE_CALENDAR = '''
SELECT CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962),112) as trade_date
FROM [BasicData].[dbo].[Yi_TradingDay]
WHERE CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962),112) >= '{start_date}'
AND CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962),112) <= '{end_date}'
ORDER BY trade_date asc
'''
