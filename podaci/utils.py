# -*- coding: utf-8 -*-
"""
Created on Fri Jan 05 10:55:34 2018

@author: ldh
"""

# utils.py

import copy
import pandas as pd

def winsorize(data):
    '''
    Winsorize function. 
    If data is a DataFrame, it works columns wise.
    
    Parameters
    ----------
    data
        DataFrame or series
    
    Returns
    -------
    Type of data, the default is a copy of data.
    
    Notes
    ------
    If data is a DataFrame, the value must be numeric.
    '''
    mean = data.mean()
    std = data.std()
    t_up = mean + 3 * std
    t_down = mean - 3 * std
    
    data_copy = copy.copy(data)
    if isinstance(data_copy,pd.DataFrame):
        for idx in t_up.index:
            data_copy[idx].loc[data_copy[idx] > t_up[idx]] = t_up[idx]
            data_copy[idx].loc[data_copy[idx] < t_down[idx]] = t_down[idx]
    elif isinstance(data_copy,pd.Series):
        data_copy.loc[data_copy > t_up] = t_up
        data_copy.loc[data_copy < t_down] = t_down
        
    return data_copy
    
    
if __name__ == '__main__':
    import numpy as np
    df = pd.DataFrame(np.random.random((20,3)))
    df_w = winsorize(df)
