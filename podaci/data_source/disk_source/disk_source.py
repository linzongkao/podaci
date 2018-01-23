# -*- coding: utf-8 -*-
"""
Created on Tue Jan 23 15:30:32 2018

@author: ldh

让vectra支持excel数据源。
"""

# disk_source.py

import numpy as np
import pandas as pd
from vectra.interface import AbstractDataSource

class DiskDataSource(AbstractDataSource):
    
    def __init__(self,universe,file_path):
        self.universe = universe
        self.file_path = file_path
        self._handle_data()
        
    def _handle_data(self):
        self.data = pd.read_excel(self.file_path,
                                  parse_dates = True,
                                  dtype = {'trade_code':str,
                                           'trade_date':str})
        self.data['trade_date'] = pd.to_datetime(self.data['trade_date'],
                 yearfirst = True)
        
        trade_date = np.array(list(set(self.data['trade_date'].values)))
        trade_date.sort()
        columns = ['open_price','high_price','low_price','close_price',
                   'amount','volume']
        self.target_dict = {'trade_date':trade_date}
        
        for attr in columns:
            tmp = pd.pivot_table(self.data,values = [attr],
                                    columns = ['trade_code'],
                                    index = ['trade_date'])
            tmp = tmp[attr]
            tmp = tmp.reindex(columns = self.universe)
            tmp = tmp.values
            self.target_dict[attr] = tmp
            
    def get_attr(self,universe,start_date = None,end_date = None,
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
    
    def get_calendar_days(self):
        return self.target_dict['trade_date']
    
    def get_trade_status(self):
        return
    
if __name__ == '__main__':
    ds = DiskDataSource('G:\\Work_ldh\\backtest_data\\data.xlsx')


