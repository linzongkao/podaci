# -*- coding: utf-8 -*-
"""
Created on Fri Sep 14 13:53:46 2018

@author: ldh

进阶的tushare数据接口
1. 首次获取tushare数据从网络请求,然后保存本地
2. 再次请求,会从本地读取数据
3. 提供本地数据更新功能
"""

# data_pro.py
import datetime as dt
import os
import yaml
import pandas as pd
import tushare as ts

# 载入配置
etc_path = os.path.join(os.path.split(__file__)[0],'etc.yaml')
with open(etc_path,'r') as f:
    etc = yaml.load(f)
tushare_token = etc['tushare_token']
data_pro_path = etc['data_pro_path']

items_path = os.path.join(os.path.split(__file__)[0],'items.txt')
with open(items_path,'r') as f:
    items = f.readline().split(',')


    
class DataPro():
    def __init__(self):
        ts.set_token(tushare_token)
        self.pro = ts.pro_api()
        self.local_items = items

    def update_data(self):
        '''
        更新本地数据.
        '''
        if 'stock_basic' in self.local_items:
            self.update_stock_basic()
        if 'trade_cal' in self.local_items:
            self.update_trade_calendar()        
        if 'daily' in self.local_items:
            self.update_daily()
        if 'adj_factor' in self.local_items:
            self.update_adj_factor()
        if 'daily_basic' in self.local_items:
            self.update_daily_basic()
            
    def _add_item(self,item_name):
        with open(items_path,'r') as f:
            content_len = len(f.readline())
        if content_len == 0:            
            with open(items_path,'a') as f:
                f.write('%s'%item_name)
        else:
            with open(items_path,'a') as f:
                f.write(',%s'%item_name)
        self.local_items.append(item_name)
        
    #%% api
    def get_stock_universe(self):
        '''
        获取股票列表。
        '''
        if 'stock_basic' in self.local_items:
            return pd.read_hdf(os.path.join(data_pro_path,'stock_basic.h5'),'stock_basic')
        else:
            fields = 'ts_code,symbol,name,list_date,delist_date,list_status'
            stock_basic = self.pro.query('stock_basic',exchange_id='', is_hs='',
                                         fields = fields)
            for each in fields.split(','):
                stock_basic[each] = stock_basic[each].apply(lambda x:None if x is None else x.encode('utf8'))
            stock_basic.to_hdf(os.path.join(data_pro_path,'stock_basic.h5'),'stock_basic',
                               mode = 'w',append = True)

            self._add_item('stock_basic')
            return stock_basic
        
    def get_trade_calendar(self):
        '''
        交易日历
        '''        
        if 'trade_cal' in self.local_items:
            return pd.read_hdf(os.path.join(data_pro_path,'trade_cal.h5'),'trade_cal')
        else:
            trade_cal = self.pro.query('trade_cal',start_date = '20000101',
                                       end_date = (dt.date.today() + dt.timedelta(days = 365)).strftime('%Y%m%d'),
                                       is_open = 1)
            trade_cal['exchange_id'] = trade_cal['exchange_id'].apply(lambda x:x.encode('utf8'))
            trade_cal['cal_date'] = trade_cal['cal_date'].apply(lambda x:x.encode('utf8'))
            trade_cal.to_hdf(os.path.join(data_pro_path,'trade_cal.h5'),'trade_cal',mode = 'w',append = True)
            self._add_item('trade_cal')
            return trade_cal
        
    def get_daily_trade(self,code,start_date,end_date):
        '''
        获取日线行情。自动判断本地读取或者远程请求。
        
        Parameters
        ----------
        code
            ts股票代码
        start_date
            开始日期
        end_date
            结束日期
            
        Returns
        --------
        DataFrame
        '''
        if 'daily' in self.local_items:
            try:
                return pd.read_hdf(os.path.join(data_pro_path,'daily.h5'),'daily_%s'%code.split('.')[0],                
                                   where = ["(index>='%s') & (index<='%s')"%(start_date,end_date)]).drop_duplicates()
            except KeyError:
                daily = self.pro.query('daily',ts_code = code,start_date = '20000101',
                                       end_date = (dt.date.today()).strftime('%Y%m%d'))
                fields = ['ts_code','trade_date']
                for each in fields:
                    daily[each] = daily[each].apply(lambda x:x.encode('utf8'))
                daily.set_index('trade_date',inplace = True)
                daily.to_hdf(os.path.join(data_pro_path,'daily.h5'),'daily_%s'%code.split('.')[0],
                             mode = 'a',append = True)
                return daily.loc[(daily.index >= start_date) & (daily.index <= end_date)]
            
        else:
            daily = self.pro.query('daily',ts_code = code,start_date = '20000101',
                                       end_date = (dt.date.today()).strftime('%Y%m%d'))
            fields = ['ts_code','trade_date']
            for each in fields:
                daily[each] = daily[each].apply(lambda x:x.encode('utf8'))
            daily.set_index('trade_date',inplace = True)
            daily.to_hdf(os.path.join(data_pro_path,'daily.h5'),'daily_%s'%code.split('.')[0],mode = 'a',append = True)
            self._add_item('daily')
            return daily.loc[(daily.index >= start_date) & (daily.index <= end_date)]
        
    def get_adjfactor(self,code,start_date,end_date):
        '''
        获取复权因子。
        '''
        if 'adj_factor' in self.local_items:
            try:
                return pd.read_hdf(os.path.join(data_pro_path,'adj_factor.h5'),'adj_factor_%s'%code.split('.')[0],
                                   where = ["(index>='%s') & (index<='%s')"%(start_date,end_date)])
            except KeyError:
                adj_factor = self.pro.query('adj_factor',ts_code = code,trade_date = '')
                
                fields = ['ts_code','trade_date']
                for each in fields:
                    adj_factor[each] = adj_factor[each].apply(lambda x:x.encode('utf8'))                   
                adj_factor.set_index('trade_date',inplace = True)
                adj_factor.to_hdf(os.path.join(data_pro_path,'adj_factor.h5'),
                                  'adj_factor_%s'%code.split('.')[0],mode = 'a',append = True)
                return adj_factor.loc[(adj_factor.index >= start_date) & (adj_factor.index <= end_date)]  
        else:
            adj_factor = self.pro.query('adj_factor',ts_code = code,trade_date = '')
            
            fields = ['ts_code','trade_date']
            for each in fields:
                adj_factor[each] = adj_factor[each].apply(lambda x:x.encode('utf8'))
                
            adj_factor.set_index('trade_date',inplace = True)
            adj_factor.to_hdf(os.path.join(data_pro_path,'adj_factor.h5'),
                              'adj_factor_%s'%code.split('.')[0],mode = 'a',append = True)
            self._add_item('adj_factor')
            return adj_factor.loc[(adj_factor.index >= start_date) & (adj_factor.index <= end_date)]  
        
    def get_daily_basic(self,code,start_date,end_date):
        '''
        获取每日指标。
        
        Parameters
        ----------
        code
            ts股票代码
        start_date

            开始日期
        end_date
            结束日期
            
        Returns
        --------
        DataFrame
        '''
        if 'daily_basic' in self.local_items:
            try:
                return pd.read_hdf(os.path.join(data_pro_path,'daily_basic.h5'),'daily_basic_%s'%code.split('.')[0],                
                                   where = ["(index>='%s') & (index<='%s')"%(start_date,end_date)])
            except KeyError:
                daily_basic = self.pro.query('daily_basic',ts_code = code,start_date = '20000101',
                                       end_date = (dt.date.today()).strftime('%Y%m%d'))
                fields = ['ts_code','trade_date']
                for each in fields:
                    daily_basic[each] = daily_basic[each].apply(lambda x:x.encode('utf8'))
                daily_basic.set_index('trade_date',inplace = True)
                daily_basic.to_hdf(os.path.join(data_pro_path,'daily_basic.h5'),'daily_basic_%s'%code.split('.')[0],mode = 'a',append = True)
                return daily_basic.loc[(daily_basic.index >= start_date) & (daily_basic.index <= end_date)]
            
        else:
            daily_basic = self.pro.query('daily_basic',ts_code = code,start_date = '20000101',
                                       end_date = (dt.date.today()).strftime('%Y%m%d'))
            fields = ['ts_code','trade_date']
            for each in fields:
                daily_basic[each] = daily_basic[each].apply(lambda x:x.encode('utf8'))
            daily_basic.set_index('trade_date',inplace = True)
            daily_basic.to_hdf(os.path.join(data_pro_path,'daily_basic.h5'),'daily_basic_%s'%code.split('.')[0],
                               mode = 'a',append = True)
            self._add_item('daily_basic')
            return daily_basic.loc[(daily_basic.index >= start_date) & (daily_basic.index <= end_date)]
    
    def get_income(self,code,start_date,end_date,report_type = 1):
        '''
        获取利润表。
		
    		Parameters
    		----------
    		code
    			ts代码
    		start_date
                公告开始日期
            end_date
                公告结束日期
            report_type
                公告类型
            
        Returns
        --------
        DataFrame        
        '''
        if 'income' in self.local_items:
            try:
                return pd.read_hdf(os.path.join(data_pro_path,'income.h5'),
                                   'income_%s_%s'%(code.split('.')[0],report_type),
                                   where = ["(index>='%s') & (index<='%s')"%(start_date,end_date)]).\
                                   drop_duplicates(['ann_date','end_date'])
            except KeyError:
                income = self.pro.query('income',ts_code = code,start_date = '20000101',
                                              end_date = (dt.date.today()).strftime('%Y%m%d')).\
                                        drop_duplicates(['ann_date','end_date'])
                
                fields = ['ts_code','ann_date','f_ann_date','end_date','report_type','comp_type']
                for each in fields:
                    income[each] = income[each].apply(lambda x:x.encode('utf8'))                   
                income.set_index('f_ann_date',inplace = True)
                income.to_hdf(os.path.join(data_pro_path,'income.h5'),
                                  'income_%s_%s'%(code.split('.')[0],report_type),
                                  mode = 'a',append = True)
                return income.loc[(income.index >= start_date) & (income.index <= end_date)]  
        else:
            income = self.pro.query('income',ts_code = code,start_date = '20000101',
                                              end_date = (dt.date.today()).strftime('%Y%m%d')).\
                                    drop_duplicates(['ann_date','end_date'])             
            fields = ['ts_code','ann_date','f_ann_date','end_date','report_type','comp_type']
            for each in fields:
                income[each] = income[each].apply(lambda x:x.encode('utf8'))                   
            income.set_index('f_ann_date',inplace = True)
            income.to_hdf(os.path.join(data_pro_path,'income.h5'),
                              'income_%s_%s'%(code.split('.')[0],report_type),
                              mode = 'a',append = True)
            self._add_item('income')
            return income.loc[(income.index >= start_date) & (income.index <= end_date)]
        
    def get_balancesheet(self,code,start_date,end_date,report_type = '1'):
        '''
        获取资产负债表。
        
        Parameters
        ----------
        code
            ts股票代码
        start_date
            公告开始日期
        end_date
            公告结束日期
        report_type
            公告类型
            
        Returns
        --------
        DataFrame        
        '''
        if 'balancesheet' in self.local_items:
            try:
                return pd.read_hdf(os.path.join(data_pro_path,'balancesheet.h5'),
                                   'balancesheet_%s_%s'%(code.split('.')[0],report_type),
                                   where = ["(index>='%s') & (index<='%s')"%(start_date,end_date)]).\
                                   drop_duplicates(['ann_date','end_date'])
            except KeyError:
                balancesheet = self.pro.query('balancesheet',ts_code = code,start_date = '20000101',
                                              end_date = (dt.date.today()).strftime('%Y%m%d')).\
                                              drop_duplicates(['ann_date','end_date'])
                
                fields = ['ts_code','ann_date','f_ann_date','end_date','report_type','comp_type']
                for each in fields:
                    balancesheet[each] = balancesheet[each].apply(lambda x:x.encode('utf8'))                   
                balancesheet.set_index('f_ann_date',inplace = True)
                balancesheet.to_hdf(os.path.join(data_pro_path,'balancesheet.h5'),
                                  'balancesheet_%s_%s'%(code.split('.')[0],report_type),
                                  mode = 'a',append = True)
                return balancesheet.loc[(balancesheet.index >= start_date) & (balancesheet.index <= end_date)]  
        else:
            balancesheet = self.pro.query('balancesheet',ts_code = code,start_date = '20000101',
                                              end_date = (dt.date.today()).strftime('%Y%m%d')).\
                                          drop_duplicates(['ann_date','end_date'])             
            fields = ['ts_code','ann_date','f_ann_date','end_date','report_type','comp_type']
            for each in fields:
                balancesheet[each] = balancesheet[each].apply(lambda x:x.encode('utf8'))                   
            balancesheet.set_index('f_ann_date',inplace = True)
            balancesheet.to_hdf(os.path.join(data_pro_path,'balancesheet.h5'),
                              'balancesheet_%s_%s'%(code.split('.')[0],report_type),
                              mode = 'a',append = True)
            self._add_item('balancesheet')
            return balancesheet.loc[(balancesheet.index >= start_date) & (balancesheet.index <= end_date)]  
        
    def get_cashflow(self,code,start_date,end_date,report_type = '1'):
        '''
        获取现金流量表。
        
        Parameters
        ----------
        code
            ts股票代码
        start_date
            公告开始日期
        end_date
            公告结束日期
        report_type
            公告类型            
        Returns
        --------
        DataFrame        
        '''
        if 'cashflow' in self.local_items:
            try:
                return pd.read_hdf(os.path.join(data_pro_path,'cashflow.h5'),
                                   'cashflow_%s_%s'%(code.split('.')[0],report_type),
                                   where = ["(index>='%s') & (index<='%s')"%(start_date,end_date)]).\
                                   drop_duplicates(['ann_date','end_date'])
            except KeyError:
                cashflow = self.pro.query('cashflow',ts_code = code,start_date = '20000101',
                                              end_date = (dt.date.today()).strftime('%Y%m%d')).\
                                          drop_duplicates(['ann_date','end_date'])
                
                fields = ['ts_code','ann_date','f_ann_date','end_date','report_type','comp_type']
                for each in fields:
                    cashflow[each] = cashflow[each].apply(lambda x:x.encode('utf8'))                   
                cashflow.set_index('f_ann_date',inplace = True)
                cashflow.to_hdf(os.path.join(data_pro_path,'cashflow.h5'),
                                  'cashflow_%s_%s'%(code.split('.')[0],report_type),
                                  mode = 'a',append = True)
                return cashflow.loc[(cashflow.index >= start_date) & (cashflow.index <= end_date)]  
        else:
            cashflow = self.pro.query('cashflow',ts_code = code,start_date = '20000101',
                                              end_date = (dt.date.today()).strftime('%Y%m%d')).\
                                              drop_duplicates(['ann_date','end_date'])             
            fields = ['ts_code','ann_date','f_ann_date','end_date','report_type','comp_type']
            for each in fields:
                cashflow[each] = cashflow[each].apply(lambda x:x.encode('utf8'))                   
            cashflow.set_index('f_ann_date',inplace = True)
            cashflow.to_hdf(os.path.join(data_pro_path,'cashflow.h5'),
                              'cashflow_%s_%s'%(code.split('.')[0],report_type),
                              mode = 'a',append = True)
            self._add_item('cashflow')
            return cashflow.loc[(cashflow.index >= start_date) & (cashflow.index <= end_date)]         
      
    def get_index_basic(self,market):
        '''
        获取指数列表。
        
        Parameters
        -----------
        market
            交易所或服务商代码
        '''
        if 'index_basic' in self.local_items:
            try:
                return pd.read_hdf(os.path.join(data_pro_path,'index_basic.h5'),'%s'%market)
            except KeyError:
                fields = 'ts_code,name,fullname,market,publisher,index_type,category,base_date,list_date,weight_rule,desc,exp_date'
                index_basic = self.pro.query('index_basic',market = market,fields = fields)
                for each in fields.split(','):
                    index_basic[each] = index_basic[each].apply(lambda x:None if x is None else x.encode('utf8'))
                index_basic.to_hdf(os.path.join(data_pro_path,'index_basic.h5'),'%s'%market,
                                   mode = 'w',append = True)
                return index_basic                
        else:
            fields = 'ts_code,name,fullname,market,publisher,index_type,category,base_date,list_date,weight_rule,desc,exp_date'
            index_basic = self.pro.query('index_basic',market = market,fields = fields)
            for each in fields.split(','):
                index_basic[each] = index_basic[each].apply(lambda x:None if x is None else x.encode('utf8'))
            index_basic.to_hdf(os.path.join(data_pro_path,'index_basic.h5'),'%s'%market,
                               mode = 'w',append = True)

            self._add_item('index_basic')
            return index_basic
        
    def get_index_daily(self,code,start_date,end_date):
        '''
        获取指数日线行情。
        
        Parameters
        ----------
        code
            ts股票代码
        start_date
            开始日期
        end_date
            结束日期
            
        Returns
        --------
        DataFrame
        '''
        if 'index_daily' in self.local_items:
            try:
                return pd.read_hdf(os.path.join(data_pro_path,'index_daily.h5'),
                                   'index_daily_%s'%('_'.join(code.split('.'))),                
                                   where = ["(index>='%s') & (index<='%s')"%(start_date,end_date)])
            except KeyError:
                index_daily = self.pro.query('index_daily',ts_code = code,start_date = '20000101',
                                       end_date = (dt.date.today()).strftime('%Y%m%d'))
                fields = ['ts_code','trade_date']
                for each in fields:
                    index_daily[each] = index_daily[each].apply(lambda x:x.encode('utf8'))
                index_daily.set_index('trade_date',inplace = True)
                index_daily.to_hdf(os.path.join(data_pro_path,'index_daily.h5'),
                                   'index_daily_%s'%('_'.join(code.split('.'))),
                                   mode = 'a',append = True)
                return index_daily.loc[(index_daily.index >= start_date) & (index_daily.index <= end_date)]
            
        else:
            index_daily = self.pro.query('index_daily',ts_code = code,start_date = '20000101',
                                       end_date = (dt.date.today()).strftime('%Y%m%d'))
            fields = ['ts_code','trade_date']
            for each in fields:
                index_daily[each] = index_daily[each].apply(lambda x:x.encode('utf8'))
            index_daily.set_index('trade_date',inplace = True)
            index_daily.to_hdf(os.path.join(data_pro_path,'index_daily.h5'),
                               'index_daily_%s'%('_'.join(code.split('.'))),
                               mode = 'a',append = True)
            self._add_item('index_daily')
            return index_daily.loc[(index_daily.index >= start_date) & (index_daily.index <= end_date)]
        
    #%% update
    def update_stock_basic(self):
        stock_basic = self.pro.query('stock_basic',fields = 'ts_code,symbol,name,list_date,delist_date,list_status')
        fields = 'ts_code,symbol,name,list_date,delist_date,list_status'
        for each in fields.split(','):
            stock_basic[each] = stock_basic[each].apply(lambda x:x.encode('utf8'))            
        stock_basic.to_hdf(os.path.join(data_pro_path,'stock_basic.h5'),mode = 'w',append = True) 
    
    def update_trade_calendar(self):
        trade_cal = self.pro.query('trade_cal',start_date = '19910101',
                                       end_date = (dt.date.today() + dt.timedelta(days = 365)).strftime('%Y%m%d'))
        trade_cal['exchange_id'] = trade_cal['exchange_id'].apply(lambda x:x.encode('utf8'))
        trade_cal['cal_date'] = trade_cal['cal_date'].apply(lambda x:x.encode('utf8'))
        trade_cal.to_hdf(os.path.join(data_pro_path,'trade_cal.h5'),'trade_cal',mode = 'w',append = True)

    def update_daily(self):
        hdf_store = pd.HDFStore(os.path.join(data_pro_path,'daily.h5'),mode = 'a')
        keys = [each[1:] for each in hdf_store.keys()]
        def add_suffix(x):
            if x.startswith('0') or x.startswith('3'):
                return x + '.SZ'
            elif x.startswith('6'):
                return x + '.SH'
        codes = map(add_suffix,
                    [each.split('_')[1] for each in hdf_store.keys()])
        
        fields = ['ts_code','trade_date']
        for idx,code in enumerate(codes):
            key = keys[idx]
            last_trade_date = hdf_store[key].index.max()
            start_date = dt.datetime.strptime(last_trade_date,'%Y%m%d') + dt.timedelta(days = 1)
            start_date = start_date.strftime('%Y%m%d')
            
            if start_date >  dt.date.today().strftime('%Y%m%d'):
                continue
            daily = self.pro.query('daily',ts_code = code,
                                      start_date = start_date,
                                      end_date = dt.date.today().strftime('%Y%m%d'))
            if len(daily) == 0:
                continue
            
            for each in fields:
                daily[each] = daily[each].apply(lambda x:x.encode('utf8'))
            daily.set_index('trade_date',inplace = True)
            hdf_store.append(key,daily)
        hdf_store.flush()
        hdf_store.close()
        
    def update_adj_factor(self):
        hdf_store = pd.HDFStore(os.path.join(data_pro_path,'adj_factor.h5'),mode = 'a')
        keys = [each[1:] for each in hdf_store.keys()]
        
        def add_suffix(x):
            if x.startswith('0') or x.startswith('3'):
                return x + '.SZ'
            elif x.startswith('6'):
                return x + '.SH'
        codes = map(add_suffix,
                    [each.split('_')[2] for each in hdf_store.keys()])
        hdf_store.close()
        fields = ['ts_code','trade_date']
        
        for idx,code in enumerate(codes):
            key = keys[idx]
            adj_factor = self.pro.query('adj_factor',ts_code = code,trade_date = '')
            
            if len(adj_factor) == 0:
                continue
            
            for each in fields:
                adj_factor[each] = adj_factor[each].apply(lambda x:x.encode('utf8'))
            adj_factor.set_index('trade_date',inplace = True)
            adj_factor.to_hdf(os.path.join(data_pro_path,'adj_factor.h5'),
                              key,mode = 'w',append = True)
        

    def update_daily_basic(self):
        hdf_store = pd.HDFStore(os.path.join(data_pro_path,'daily_basic.h5'),mode = 'a')
        keys = [each[1:] for each in hdf_store.keys()]
        def add_suffix(x):
            if x.startswith('0') or x.startswith('3'):
                return x + '.SZ'
            elif x.startswith('6'):
                return x + '.SH'
        codes = map(add_suffix,
                    [each.split('_')[2] for each in hdf_store.keys()])
        
        fields = ['ts_code','trade_date']
        for idx,code in enumerate(codes):
            key = keys[idx]
            last_trade_date = hdf_store[key].index.max()
            start_date = dt.datetime.strptime(last_trade_date,'%Y%m%d') + dt.timedelta(days = 1)
            start_date = start_date.strftime('%Y%m%d')
            
            if start_date >  dt.date.today().strftime('%Y%m%d'):
                continue
            daily_basic = self.pro.query('daily_basic',ts_code = code,
                                      start_date = start_date,
                                      end_date = dt.date.today().strftime('%Y%m%d'))
            if len(daily_basic) == 0:
                continue
            
            for each in fields:
                daily_basic[each] = daily_basic[each].apply(lambda x:x.encode('utf8'))
            daily_basic.set_index('trade_date',inplace = True)
            hdf_store.append(key,daily_basic)
        hdf_store.flush()
        hdf_store.close() 
        
if __name__ == '__main__':
    dp = DataPro()
    