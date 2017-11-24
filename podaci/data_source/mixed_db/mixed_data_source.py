# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 10:22:28 2017

@author: ldh
"""
import time

import pandas as pd

from vectra.constants import EASY_MODE,HARD_MODE
from vectra.interface import AbstractDataSource
from database_engine import DatabaseEngine
from sqls import SQL_GET_TRADING_DATA
from utils import func_1,func_2

#%% 连接数据库
engine_obj = DatabaseEngine('guosen')
engine = engine_obj.get_engine()
session = engine_obj.get_session()

#%% 混合数据源
class MixedDataSource(AbstractDataSource):
    
    def __init__(self,universe,start_date,end_date,data_mode):
        '''
        初始化回测所需数据。
        '''
        t1 = time.time()
        self.universe = universe
        self.start_date = start_date
        self.end_date = end_date
        self.data_mode = data_mode
        
        if self.data_mode == EASY_MODE:
            self.data = pd.read_sql(SQL_GET_TRADING_DATA%("("+ str(self.universe)[1:-1] + ")",
                                                          self.start_date,self.end_date),
                        engine)
        elif self.data_mode == HARD_MODE:
            pass
        
        self._handle_data()
        t2 = time.time()
        print 'Preparing data successfully, cost %.2f seconds...'%(t2 - t1)
        
    def _handle_data(self):
        '''
        将数据处理成需要的格式供内部api调用。
        '''
        self.data['trade_status'] = func_2(self.data['trade_status'])
        data_group = self.data.groupby('ticker',sort = False)
        self.data_pre_gened = (data_group.apply(func_1)).reset_index(drop = True)
        self.data_pivoted = pd.pivot_table(self.data_pre_gened,index = ['trade_date'],
                                           columns = ['ticker'],values = ['open_price',
                                                     'high_price','low_price','close_price',
                                                     'amount','volume'])
        self.data_pivoted = self.data_pivoted.reindex(level=1,columns = self.universe)
        self.data_dict = {}
        for col in self.data_pivoted.columns.levels[0]:
            self.data_dict[col] = self.data_pivoted[col].values
        self.data_dict['trade_date'] = self.data_pivoted.index.to_pydatetime()
        
    def get_attr(self,universe,start_date,end_date,frequency,data_mode):
        '''
        按属性获取时间、高、开、低、收、成交量、成交金额数据。
        
        Parameters
        -----------
        universe
            list,股票池 ['600340','000001']
        start_date
            '20100101'
        end_date
            '20150101'
        frequency
            '1d','1m','3m',...
        data_mode
            EASY_MODE,HARD_MODE

        Returns
        --------
        dict
            keys : 
                date_time,open_price,high_price,low_price,close_price,amount,volume 
            value : 
                np.array
                
        Notes
        ------
        数据按列对应universe
        '''
        return self.data_dict
        
        
    def get_calendar_days(self,start_date,end_date):
        '''
        返回start_date到end_date间的交易日。
		
        Parameters
        -----------
    		start_date
        		'20100101'
    		end_date
        		'20150101'
                
        Returns
        -------
    		list 
            [datetime]
        '''
        return pd.Index(self.data['trade_date'].sort_values().unique()).to_pydatetime()
    
    def get_trade_status(self,universe,start_date,end_date):
        '''
        获取股票交易状态。
    		
        Parameters
        -----------
        universe
            stocks
        start_date
            '20100101'
        end_date
            '20150101'
                
        Returns
        --------
        DataFrame 
            index 
                datetime,'trade_date'
            columns
                (ticker1,ticker2,...,tickern)
            values
                [[0,1,1,1],[0,0,0,0]]
        Notes
        -------
        status 
        正常交易: 1
        停牌或未上市或已退市: 0
        '''
        trade_status = self.data[['ticker','trade_date','trade_status']]
        trade_status = trade_status.pivot(index = 'trade_date',
                                          columns = 'ticker')
        trade_status = trade_status.reindex(columns = self.universe,
                                            level = 1)
        return trade_status['trade_status']
    
if __name__ == '__main__':
    uni = ['600340']
    start_date = '20160102'
    end_date = '20170101'
    mds = MixedDataSource(uni,start_date,end_date,EASY_MODE)

