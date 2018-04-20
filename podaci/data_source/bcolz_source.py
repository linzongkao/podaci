# -*- coding: utf-8 -*-
"""
Created on Mon Mar 26 09:11:17 2018

@author: ldh
"""

import numpy as np
import pandas as pd
import bcolz
from vectra.interface import AbstractDataSource

class BcolzDataSource(AbstractDataSource):
    
    def __init__(self,config):
        self.config = config
        self.start_date = self.config.start_date
        self.end_date = self.config.end_date
        self.universe = config.universe
        self.file_path = config.file_path
        self._handle_data()
        
    def _handle_data(self):
        self.data = bcolz.open(self.file_path,mode = 'r')
        self.data = self.data.todataframe()
        self.trade_date = self.data['trade_date'].unique()
        self.data = self.data.loc[(self.data['trade_date'] >= self.start_date) & \
                                  (self.data['trade_date'] <= self.end_date)]
        self.data.loc[:,'trade_date'] = pd.to_datetime(self.data['trade_date'])
        self.trade_date.sort() 
        self.trade_date = pd.to_datetime(self.trade_date)
        self.trade_date = self.trade_date[(self.data['trade_date'] >= self.start_date) & \
                                  (self.data['trade_date'] <= self.end_date)]
        self.target_dict = {'trade_date':np.array([each.to_pydatetime() for \
                                                   each in self.trade_date])}
    
        columns1 = ['open_price','high_price','low_price','close_price']
        columns2 = ['amount','volume']   
        
        for attr in columns1:
            tmp = pd.pivot_table(self.data,values = [attr],
                                    columns = ['trade_code'],
                                    index = ['trade_date'],
                                    fill_value = None)
            tmp = tmp[attr]
            tmp = tmp.reindex(self.trade_date)
            tmp = tmp.reindex(columns = self.universe)
            tmp = tmp.fillna(method = 'pad')
            tmp = tmp.fillna(value = 0.0)
            tmp = tmp.values
            self.target_dict[attr] = tmp
            
        for attr in columns2:
            tmp = pd.pivot_table(self.data,values = [attr],
                                    columns = ['trade_code'],
                                    index = ['trade_date'],
                                    fill_value = 0.0)
            tmp = tmp[attr]
            tmp = tmp.reindex(self.trade_date,fill_value = 0.0)
            tmp = tmp.reindex(columns = self.universe)
            tmp = tmp.values
            self.target_dict[attr] = tmp 
            
        
            
    def get_attr(self,universe = None,start_date = None,end_date = None,
                 frequency = None,data_mode = None):
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
                trade_date,open_price,high_price,low_price,close_price,amount,volume
            value : 
                np.array
                
        Notes
        ------
        数据按列对应universe
        '''
        return self.target_dict
    
    def get_calendar_days(self,start_date,end_date):
        return np.array([each.to_pydatetime() for each in self.trade_date])
    
    def get_trade_status(self,universe,start_date,end_date):
        tmp = pd.pivot_table(self.data,values = ['open_price'],
                             index = ['trade_date'],
                             columns = ['trade_code'],
                             fill_value = 0.0)
        tmp = tmp['open_price']
        tmp = tmp.loc[(tmp.index <= ds.trade_date[-1]) & \
                      (tmp.index >= ds.trade_date[0])]
        tmp[tmp > 0] = 1.0
        tmp.index = tmp.index.to_pydatetime()
        return tmp
    
if __name__ == '__main__':
    from vectra.utils.parse_config import Config
    file_path = 'G:\\Work_ldh\\Projects\\yi_portfolio\\yi_invest\\data'
    universe = bcolz.open(file_path,mode = 'r').todataframe()
    universe = universe['trade_code'].unique().tolist()
    config = {'base':{'start_date':'20130101',
                  'end_date':'20171231',
                  'capital':1000000.0,
                  'frequency':'1d',
                  'universe':universe},
          'source':'bcolz',
          'file_path':file_path}
    config = Config(config)
    ds = BcolzDataSource(config,universe,file_path)
