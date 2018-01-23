# -*- coding: utf-8 -*-
"""
Created on Tue Jan 09 10:27:16 2018

@author: ldh
"""

# factor_data_source.py

from podaci.wind.wind_api import get_wsd

class FactorDataSource():
    '''
    Get data to feed missfactor.
    '''
    def __init__(self):
        pass
    
    def get_ret(self,universe,start_date,end_date,freq):
        '''
        Return the return data.
        
        Parameters
        ----------
        universe
            list,security universe
        start_date
            str,'20180101'
        end_date
            str,'20181001'
        freq
            str, 'Y':yearly,'M':monthly,'W':weekly,'D':daily
            
        Returns
        --------
        DataFrame
            index:datetime , columns:security id
        '''
        data = get_wsd(universe,'close',start_date,end_date,Period = freq,
                       PriceAdj = 'B')
        data = data.sort_index(ascending = True)
        data = data.pct_change().dropna()
        return data
    
    def get_factor(self,universe,factor,start_date,end_date,freq):
        '''
        Return the return data.
        
        Parameters
        ----------
        universe
            list,security universe
        factor
            str,factor name
        start_date
            str,'20180101'
        end_date
            str,'20181001'
        freq
            str, 'Y':yearly,'M':monthly,'W':weekly,'D':daily
                
        Returns
        --------
        DataFrame
            index:datetime , columns:security id        
        '''
        data = get_wsd(universe,factor,start_date,end_date,Period=freq,
                       Fill='Previous')
        return data