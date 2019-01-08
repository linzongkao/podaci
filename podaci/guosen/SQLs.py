# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 13:16:09 2018

@author: ldh
"""

# SQLs.py

#%% 基础
SQL_GET_TRADE_CALENDAR = '''
SELECT CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962),112) as trade_date
FROM [BasicData].[dbo].[Yi_TradingDay]
WHERE CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962),112) >= '{start_date}'
AND CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962),112) <= '{end_date}'
ORDER BY trade_date asc
'''

#%% 股票
SQL_GET_STOCK_BASICS = '''
SELECT 
[股票名称] as stock_name
,[发布日期] as publish_date
,[交易所] as exchange
,[股票代码] as stock_code
,[上市日期] as list_date
,[摘牌日期] as delist_date
,[是否摘牌] as if_delist
,[是否上市] as if_list
FROM [BasicData].[dbo].[Yi_Stocks]
WHERE [是否上市] = 1
'''

SQL_GET_STOCK_MIN_DATA = '''
SELECT 
CONVERT(VARCHAR(32),CONVERT(DATETIME,table_close.[numtime] - 693962 + 0.0001/9),120) as trade_date
,table_close.[stockcode]
,table_close.[market]
,table_open.value as open_price
,table_close.[value] as close_price
,table_high.value as high_price
,table_low.value as low_price
,table_amount.value as amount
FROM [BasicData].[dbo].[Yi_1mClose] table_close
INNER JOIN [Yi_1mOpen] table_open ON (table_open.numtime = table_close.numtime AND table_open.stockcode = table_close.stockcode)
INNER JOIN [Yi_1mHigh] table_high ON (table_high.numtime = table_close.numtime AND table_high.stockcode = table_close.stockcode)
INNER JOIN [Yi_1mLow] table_low ON (table_low.numtime = table_close.numtime AND table_low.stockcode = table_close.stockcode)
INNER JOIN [Yi_1mAmount] table_amount ON (table_amount.numtime = table_close.numtime AND table_amount.stockcode = table_close.stockcode)
WHERE 
'''

SQL_GET_STOCK_MIN_DATA_OPEN = '''
SELECT 
CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) as trade_dt
,[stockcode]
,[market]
,value as open_price
FROM [BasicData].[dbo].[Yi_1mOpen]
WHERE CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) >= '{start_dt}'
AND CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) <= '{end_dt}'
AND stockcode IN ({stock_universe})
'''

SQL_GET_STOCK_MIN_DATA_HIGH = '''
SELECT 
CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) as trade_dt
,[stockcode]
,[market]
,value as high_price
FROM [BasicData].[dbo].[Yi_1mHigh]
WHERE CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) >= '{start_dt}'
AND CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) <= '{end_dt}'
AND stockcode IN ({stock_universe})
'''

SQL_GET_STOCK_MIN_LOW = '''
SELECT 
CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) as trade_dt
,[stockcode]
,[market]
,value as low_price
FROM [BasicData].[dbo].[Yi_1mOpen]
WHERE CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) >= '{start_dt}'
AND CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) <= '{end_dt}'
AND stockcode IN ({stock_universe})
'''

SQL_GET_STOCK_MIN_CLOSE = '''
SELECT 
CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) as trade_dt
,CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 ),112) as trade_date
,[stockcode]
,[market]
,value as close_price
FROM [BasicData].[dbo].[Yi_1mClose{year}]
WHERE CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) >= '{start_dt}'
AND CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 + 0.0001/9),120) <= '{end_dt}'
AND stockcode IN ({stock_universe})
'''



SQL_GET_STOCK_DAILY_DATA1 = '''
SELECT CONVERT(VARCHAR(32),CONVERT(DATETIME,t_c.[numtime] - 693962 ),112) as trade_date
,t_c.[stockcode] as stock_code
,t_c.[market]
,t_c.[value] as close_price
,t_o.value as open_price
,t_l.value as low_price
,t_h.value as high_price
,t_a.value as amount
FROM [BasicData].[dbo].[Yi_DayClose] t_c
LEFT JOIN [Yi_DayOpen] t_o ON (t_c.numtime = t_o.numtime AND t_c.stockcode = t_o.stockcode)
LEFT JOIN [Yi_DayHigh] t_h ON (t_c.numtime = t_h.numtime AND t_c.stockcode = t_h.stockcode)
LEFT JOIN [Yi_DayLow] t_l ON (t_c.numtime = t_l.numtime AND t_c.stockcode = t_l.stockcode)
LEFT JOIN [Yi_DayAmount] t_a ON (t_c.numtime = t_a.numtime AND t_c.stockcode = t_a.stockcode)
WHERE  CONVERT(VARCHAR(32),CONVERT(DATETIME,t_c.[numtime] - 693962 ),112) >= '{start_date}'
AND CONVERT(VARCHAR(32),CONVERT(DATETIME,t_c.[numtime] - 693962 ),112) <= '{end_date}'
'''

SQL_GET_STOCK_DAILY_DATA2 = '''
SELECT CONVERT(VARCHAR(32),CONVERT(DATETIME,t_c.[numtime] - 693962 ),112) as trade_date
,t_c.[stockcode] as stock_code
,t_c.[market] 
,t_c.[value] as close_price
,t_o.value as open_price
,t_l.value as low_price
,t_h.value as high_price
,t_a.value as amount
FROM [BasicData].[dbo].[Yi_DayClose] t_c
LEFT JOIN [Yi_DayOpen] t_o ON (t_c.numtime = t_o.numtime AND t_c.stockcode = t_o.stockcode)
LEFT JOIN [Yi_DayHigh] t_h ON (t_c.numtime = t_h.numtime AND t_c.stockcode = t_h.stockcode)
LEFT JOIN [Yi_DayLow] t_l ON (t_c.numtime = t_l.numtime AND t_c.stockcode = t_l.stockcode)
LEFT JOIN [Yi_DayAmount] t_a ON (t_c.numtime = t_a.numtime AND t_c.stockcode = t_a.stockcode)
WHERE  CONVERT(VARCHAR(32),CONVERT(DATETIME,t_c.[numtime] - 693962 ),112) >= '{start_date}'
AND CONVERT(VARCHAR(32),CONVERT(DATETIME,t_c.[numtime] - 693962 ),112) <= '{end_date}'
AND t_c.[stockcode] IN ({stock_universe})
'''

SQL_GET_STOCK_FEATURES = '''
SELECT 
[stock_code]
,[trade_date]
,[b]
,[c]
,[u]
,[l]
,[ol]
,[cl]
,[ac]
FROM [LDH_features]
WHERE trade_date >= '{start_date}'
AND trade_date <= '{end_date}'
AND stock_code IN ({stock_universe})
'''

SQL_GET_STOCK_ADJFACTOR = '''
SELECT CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 ),112) as trade_date
,[stockcode] as stock_code
,[value] as factor
FROM [BasicData].[dbo].[Yi_RecoveredGene]
WHERE stockcode = '{stock_code}'
AND CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 ),112) <= '{end_date}'
AND CONVERT(VARCHAR(32),CONVERT(DATETIME,[numtime] - 693962 ),112) >= '{start_date}'
'''

#%% 指数
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

SQL_GET_INDEX_DAILY = '''
SELECT CONVERT(VARCHAR(32),CONVERT(DATETIME,a.[numtime] - 693962 + 0.0001/9),112) as trade_date
,a.[stockcode] as index_code
,a.[value] as close_price
,b.[value] as high_price
,c.[value] as low_price
,d.[value] as open_price
,e.[value] as volume
,f.[value] as amount
FROM [BasicData].[dbo].[Yi_IndexDayClose] a
LEFT JOIN Yi_IndexDayHigh b ON (a.stockcode = b.stockcode and a.numtime = b.numtime)
LEFT JOIN Yi_IndexDayLow c ON (a.stockcode = c.stockcode and a.numtime = c.numtime)
LEFT JOIN Yi_IndexDayOpen d ON (a.stockcode = d.stockcode and a.numtime = d.numtime)
LEFT JOIN Yi_IndexDayVol e ON (a.stockcode = e.stockcode and a.numtime = e.numtime)
LEFT JOIN Yi_IndexDayAmount f ON (a.stockcode = f.stockcode and a.numtime = f.numtime)
WHERE a.stockcode = '{index_code}'
AND  CONVERT(VARCHAR(32),CONVERT(DATETIME,a.[numtime] - 693962),112) >= '{start_date}'
AND  CONVERT(VARCHAR(32),CONVERT(DATETIME,a.[numtime] - 693962),112) <= '{end_date}'
'''

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

#%% 基金
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

SQL_GET_FUND_MANAGER = '''
SELECT 
[trade_code]
,[name]
,[appointment_date]
,[quit_date]
,[brief_introduction]
,[gender]
,[birthday]
,[degree]
,[nationality]
,[manager_id]
,[fund_name]
,[update_datetime]
,[obj_id]
FROM [sds209635243_db].[dbo].[fund_manager]
WHERE trade_code IN ({fund_universe})
AND appointment_date < '{trade_date}'
AND(CASE WHEN (quit_date is null) then '23000101' else quit_date end) > '{trade_date}' 
'''

SQL_GET_MANAGER_FUND = '''
SELECT 
[trade_code]
,[name]
,[appointment_date]
,[quit_date]
,[brief_introduction]
,[gender]
,[birthday]
,[degree]
,[nationality]
,[manager_id]
,[fund_name]
,[update_datetime]
,[obj_id]
FROM [sds209635243_db].[dbo].[fund_manager]
WHERE obj_id IN ({managers_ids})
AND appointment_date <  '{trade_date}'
AND (CASE WHEN (quit_date is null) then '21000101' else quit_date end) > '{trade_date}' 
'''

SQL_GET_FUNDS_DAILY_RET = '''
SELECT [trade_code]
,[trade_date]
,[daily_return]
,[investment_type]
FROM [sds209635243_db].[dbo].[fund_hist_daily_return]
WHERE trade_code IN ({universe_str})
AND trade_date >= '{start_date}'
AND trade_date <= '{end_date}'
'''

SQL_GET_ALL_FUNDS_DAILY_RET = '''
SELECT [trade_code]
,[trade_date]
,[daily_return]
,[investment_type]
FROM [sds209635243_db].[dbo].[fund_hist_daily_return]
WHERE trade_date >= '{start_date}'
AND trade_date <= '{end_date}'
'''

#%% 宏观
# 一年期定期存款利率
SQL_GET_RISK_FREE_RATE = '''
SELECT F2_1255 as interest_rate,
F3_1255 as change_date,
TB_OBJECT_1257.OB_OBJECT_NAME_1257 as item_name
FROM TB_OBJECT_1255
LEFT JOIN TB_OBJECT_1257 ON TB_OBJECT_1255.F1_1255=TB_OBJECT_1257.F1_1257
WHERE TB_OBJECT_1257.OB_OBJECT_NAME_1257 = '个人定期(整存整取)一年'
ORDER BY F3_1255 DESC
'''
