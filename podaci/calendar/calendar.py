# -*- coding: utf-8 -*-
"""
Created on Fri Nov 03 15:46:20 2017

@author: ldh
"""

# calendar.py

import os
import datetime as dt
import pandas as pd

current_path = os.path.dirname(__file__)
current_path = os.path.join(current_path,'calendar.csv')

class Calendar():
    def __init__(self):
        self.calendar = pd.read_csv(current_path,parse_dates = ['date'],
                                    usecols = ['date'])
        
    def roll_date(self,date,n):
        '''
        获取日期date n日后的日期
        
        Parameters
        -----------
        date
            datetime
        n
            + ,前移 如20170103 + 1 = 20170104
            - ,后移 如20170103 - 1 = 20161231
            
        Returns
        --------
        datetime
            移动后的交易日
        '''
        idx = self.calendar.loc[self.calendar['date'] == date].index[0]
        idx += n
        return self.calendar['date'].iloc[idx].to_pydatetime()
    
    def get_latest_tradedate(self):
        '''
        获取日历上最新的日期。
        
        Returns
        --------
        datetime
        '''
        return self.calendar['date'].iloc[-1].to_pydatetime()
    
    
        
