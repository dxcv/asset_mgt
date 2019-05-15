# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 11:08:00 2019

@author: ldh
"""

# data_proxy.py

class DataProxy():
    '''
    代理
    '''
    def __init__(self,data_source,save_source):
        self.data_source = data_source
        self.save_source = save_source
        
    def pre_load(self):
        '''
        预加载数据.
        '''
        self.data_source.pre_load()
        
    # 数据输入
    def load_status(self,customer_id,strategy_id):
        '''
        读取策略运行状态.
        
        Parameters
        ----------
        customer_id
            客户id
        strategy_id
            策略id
            
        Returns
        -------
        Status
            Status对象
        '''
        return self.data_source.load_status(customer_id,strategy_id)
    
    def get_daily_pct(self,universe,trade_date):
        '''
        获取universe中在交易日当天的收益率.
        
        Parameters
        ----------
        univere
            list
        trade_date
            str,YYYYMMDD
            
        Returns
        --------
        dict,key为标的代码,value为涨跌幅
        '''
        return self.data_source.get_daily_pct(universe,trade_date)
    
    def create_trade_calendar(self,last_trade_date):
        '''
        生成交易日历.
        
        Parameters
        ---------
        last_trade_date
            YYYYMMDD,上一交易日
            
        Returns
        --------
        返回上一交易日至今的交易日历，不包含上一交易日
        '''
        return self.data_source.create_trade_calendar(last_trade_date)
    
    def move_trade_date(self,trade_date,n):
        '''
        根据交易日历移动日期。
        
        Parameters
        ----------
        trade_date
            YYYYMMDD
        n
            负数代表过去，正数代表未来
            
        Returns
        --------
        Datetime
        '''
        return self.data_source.move_trade_date(trade_date,n)
    
    def get_daily_quote_index(self,index_code,start_date,end_date):
        '''
        获取指数日行情。
        
        Parameters
        ----------
        universe
            list,标的池
        start_date
            开始日期
        end_date
            结束日期
            
        Returns
        --------
        DataFrame,index为DatetimeIndex,columns为属性,其中close_price代表收盘价
        '''
        return self.data_source.get_daily_quote_index(index_code,start_date,end_date)
    
    def get_history_close_data(self,universe,start_date,end_date):
        '''
        获取历史收盘行情.
        
        Parameters
        ----------
        universe
            list,标的池
        start_date
            开始日期
        end_date
            结束日期
            
        Returns
        --------
        DataFrame,index为DatetimeIndex,columns为标的代码
        '''
        return self.data_source.get_history_close_data(universe,start_date,end_date)
    
    def load_P(self,customer_id,strategy_id):
        '''
        读取客户P矩阵.
        
        Parameters
        ----------
        strategy_id
            str
        strategy_id
            str
        '''
        return self.data_source.load_P(customer_id,strategy_id)
    
    def load_Q(self,customer_id,strategy_id):
        '''
        读取客户Q矩阵.
        
        Parameters
        ----------
        strategy_id
            str
        strategy_id
            str
        '''
        return self.data_source.load_Q(customer_id,strategy_id)
    
    # 数据输出
    def write_into_db(self,customer_id,strategy_id,weight_hist,weight_dates,
                      rebalance_hist,rebalance_dates,net_value_record):
        '''
        写入数据库.
        '''
        self.save_source.write_into_db(customer_id,strategy_id,weight_hist,weight_dates,
                                       rebalance_hist,rebalance_dates,net_value_record)
    
    def strategy_status_first_into_DB(self,customer_id,strategy_id,start_date,rebalance_freq):    
        '''
        策略状态首次存入数据库。
        '''
        self.save_source.strategy_status_first_into_DB(customer_id,strategy_id,
                                                       start_date,rebalance_freq)
        
    def save_status(self,status):
        self.save_source.save_status(status)
    
    def get_wsd(self,universe,factor,start_date,end_date,names = None,if_convert = False,**options):
        '''
        万德wsd函数代理
        '''
        return self.data_source.get_wsd(universe,factor,start_date,end_date,names,if_convert,**options)

    def get_annual_ret(self):
        return self.data_source.get_annual_ret()
    
    def get_cov_mat(self):
        return self.data_source.get_cov_mat()
    
    def get_market_cap_ashare(self,universe,start_date,end_date):
        return self.data_source.get_market_cap_ashare(universe,start_date,end_date)