# -*- coding: utf-8 -*-
"""
Created on Fri Nov 03 16:09:07 2017

@author: ldh
"""

# refresh_calendar.py

import datetime as dt
import tushare as ts

today = dt.datetime.today().strftime('%Y-%m-%d')
new_cal = ts.get_k_data('000001',index = True,start = '19910101',end = today)
new_cal.to_csv('calendar.csv')
