# -*- coding: utf-8 -*-
"""
Created on Tue Jul 03 09:29:43 2018

@author: ldh
"""

# get_data.py

import datetime as dt

from podaci.wind.wind_api import get_wsd,get_wset,prepare_backtest_data

data_save_path = 'G:\\Work_ldh\\Data\\tradeData'
update_log_path = 'G:\\Work_ldh\\Data\\tradeData\\update_log.log'

universe_a = get_wset('sectorconstituent',
                      date=dt.date.today().strftime('%Y-%m-%d'),
                      sectorid='a001010100000000')




