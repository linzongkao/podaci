# -*- coding: utf-8 -*-
"""
Created on Thu Nov 30 14:39:12 2017

@author: ldh
"""

# calendar.py

from wind_api import get_tdays

class Calendar():
    
    def __init__(self,start_date = '20100101',end_date = '20200101',
                 exchange = None):
        self.start_date = start_date
        self.end_date = end_date
        self.exchange = exchange
        self.calendar = None
        self._fetch_calendar()
        
    def _fetch_calendar(self):
        if self.exchange:
            self.calendar = get_tdays(self.start_date,self.end_date,
                                  TradingCalendar = self.exchange)
        else:
            self.calendar = get_tdays(self.start_date,self.end_date)
            
    
    def move(self,date,n = None,days = None,months = None,years = None):
        '''
        根据交易日历移动date.
        
        Parameters
        ----------
        date
            datetime
        n
            int/float,根据日历移动的数量
        days
            int/float,移动天数,默认为None
        months
            int/float,移动月数,默认为None
        years
            int/float,移动年数,默认为None
            
        Returns
        --------
        datetime
        
        Notes
        -------
        正为未来,负为过去
        目前仅支持n.
        '''
        ind = self.calendar[self.calendar == date].index[0]
        
        if n:
            t_ind = ind + n
            return self.calendar.iat[t_ind].to_pydatetime()
        
        if days:
            pass
        
        if months:
            pass
        
        if years:
            pass    
            
if __name__ == '__main__':
    a = Calendar('20010101','20171001')
    