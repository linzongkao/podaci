# -*- coding: utf-8 -*-
"""
Created on Wed Jan 17 09:27:09 2018

@author: ldh
"""

# plot_box.py

from matplotlib.finance import candlestick_ohlc
from matplotlib.dates import date2num

from podaci.wind.wind_api import get_wsd

def plot_candle():
    '''
    做K线图.
    '''
    pass
    
if __name__ == '__main__':
    start_date = '20090731'
    end_date = '20171231'
    idx_data = get_wsd(['000300.SH'],'open,high,low,close',start_date,end_date)
