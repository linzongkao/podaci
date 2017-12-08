# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 10:21:58 2017

@author: ldh
"""

# sqls.py


#%% 国信数据库
SQL_GET_TRADING_DATA = '''
SELECT
TradingCode  as ticker,
TradingDay as trade_date,
TradingState as trade_status,
OpenPrice as open_price,
HighestPrice as high_price,
LowestPrice as low_price,
LatestPrice as close_price,
TransactionNum as amount,
TransactionVol as volume,
RecoveredGene as recover_gene
FROM LC_DailyQuoteRecoverd
INNER JOIN SecuMain ON SecuMain.SecuID = LC_DailyQuoteRecoverd.SecuID
WHERE TradingCode IN %s
AND TradingDay >= '%s'
AND TradingDay <= '%s'
'''

SQL_GET_CALENDAR = '''
SELECT TradingDay as trade_date
FROM TradingDate
WHERE TradingDay >= '%s'
AND TradingDay <= '%s'
AND IsTradingDay = 1
AND Exchange = 'SS'
'''