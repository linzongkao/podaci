# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 16:02:04 2017

@author: ldh
"""

# utils.py

import numpy as np

def wind_symbol_2_code(wind_symbol):
    return wind_symbol[:6]

def code_2_wind_symbol(code):
    if code[0] == '6':
        wind_symbol = code + '.SH'
    else:
        wind_symbol = code + '.SZ'
    return wind_symbol

def dict_2_str(d):
    final_str = ''
    for key,value in d.items():
        final_str += str(key) + '=' + str(value) + ';'
    return final_str[:-1]

wind_symbol_2_code = np.frompyfunc(wind_symbol_2_code,1,1)
code_2_wind_symbol = np.frompyfunc(code_2_wind_symbol,1,1)

if __name__ == '__main__':
    universe = ['600340','000001']
    universe_wind = code_2_wind_symbol(universe)
    d = {'industryType':1}
    a = dict_2_str(d)
