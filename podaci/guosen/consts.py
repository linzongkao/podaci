# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 16:07:51 2017

@author: ldh
"""

# const.py

DB_STATE = 'mssql+pymssql://{user}:{pws}@{server}/{db}'


SW_INDUSTRY_FIRST_CODE = ['801010.SI','801020.SI','801030.SI','801040.SI',
                          '801050.SI','801080.SI','801110.SI','801120.SI',
                          '801130.SI','801140.SI','801150.SI','801160.SI',
                          '801170.SI','801180.SI','801200.SI','801210.SI',
                          '801230.SI','801710.SI','801720.SI','801730.SI', 
                          '801740.SI','801750.SI','801760.SI','801770.SI', 
                          '801780.SI','801790.SI','801880.SI','801890.SI']
SW_INDUSTRY_FIRST_NAME = [u'农林牧渔', u'采掘', u'化工', u'钢铁', u'有色金属', 
                          u'电子', u'家用电器', u'食品饮料', u'纺织服装', 
                          u'轻工制造', u'医药生物', u'公用事业', u'交通运输',
                          u'房地产', u'商业贸易', u'休闲服务', u'综合', 
                          u'建筑材料', u'建筑装饰', u'电器设备', u'国防军工',
                          u'计算机', u'传媒', u'通信', u'银行', u'非银金融', 
                          u'汽车', u'机械设备']