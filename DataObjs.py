# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 11:26:02 2019

@author: ldh
"""

# DataObjs.py
import os
import datetime as dt
import pymssql
import json
import pandas as pd
from core import Status
from dataflow.wind.wind_api import get_tdays,get_wsd
from dataflow.api_dailyquote import get_daily_quote_index
from dataflow.wind.calendar import Calendar


class LocalMVDataSource1():
    '''
    MV模型研发环境中的本地数据源.
    '''       
    def __init__(self):
        self.calendar = Calendar(path = 'D:\Work\calendar')
        self.history_data = pd.read_excel('MVDATA.xlsx')
        
    def load_status(self,strategy_id):
        '''
        读取策略运行状态.
        
        Parameters
        ----------
        strategy_id
            策略id
            
        Returns
        -------
        Status
            Status对象
        '''
        status = Status(strategy_id)
        loaded_status = load_status_from_GB(strategy_id)
        
        if loaded_status is None:
            status.if_exists = False
            return status
        
        status.last_trade_date = loaded_status['last_trade_date']
        status.last_rebalance_date = loaded_status['last_rebalance_date']
        
        if len(loaded_status['position_weight']) == 0:
            status.weight = {}
        else:            
            status.weight = json.loads(loaded_status['position_weight'])
            
        status.rebalance_freq = loaded_status['rebalance_freq']
        status.last_net_value = float(loaded_status['last_net_value'])  
        return status
        
    def prepare_data(self,universe,start_date,end_date):
        '''
        预加载股票收盘价数据。
        
        Parameters
        ----------
        universe
            list,标的池
        start_date
            开始日期
        end_date
            结束日期
            
        Returns
        -------
        DataFrame
        '''
        return self.history_data
    
    def get_daily_pct(self,universe,trade_date,prepared_data = None):
        '''
        获取universe中在交易日当天的收益率.
        
        Parameters
        ----------
        univere
            list
        trade_date
            str,YYYYMMDD
        prepared_data
            DataFrame,预加载数据
            
        Returns
        --------
        dict,key为标的代码,value为涨跌幅
        '''
        if prepared_data is not None:
            pct = prepared_data.pct_change().dropna()
            try:
                return pct.loc[dt.datetime.strptime(trade_date,'%Y%m%d').date()].to_dict()
            except KeyError:
                return None
    
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
        
        calendar = get_tdays(last_trade_date,dt.datetime.today().strftime('%Y%m%d')).\
        apply(lambda x: x.strftime('%Y%m%d'))
        return calendar
    
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
        return self.calendar.move(trade_date,n)
        
    
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
        return get_daily_quote_index(index_code,start_date,end_date)
    
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
        return self.history_data.loc[start_date:end_date]
    
class LocalMVSaveSource1(): 
    '''
    MV模型本地存储源。
    '''
    def __init__(self,save_path):
        self.save_path = save_path
    
    # 数据输出
    def write_into_db(self,strategy_id,weight_record,rebalance_record,net_value_record):
        '''
        写入数据库.
        '''
        net_value_record.to_excel(os.path.join(self.save_path,strategy_id + '_' + 'net_value.xlsx'))
        weight_record.to_excel(os.path.join(self.save_path,strategy_id + '_' +'weight.xlsx'))
        rebalance_record.to_excel(os.path.join(self.save_path,strategy_id + '_' +'rebalance.xlsx'))
    
    def strategy_status_first_into_DB(self,strategy_id,start_date,rebalance_freq):    
        '''
        策略状态首次存入数据库。
        '''
        conn = pymssql.connect(server = '172.24.153.43',
                               user = 'DBadmin',
                               password = 'fs95536!',
                               database = 'GeniusBar')
        cursor = conn.cursor()
        cursor.execute('''
           INSERT INTO strategy_status
        (strategy_id,last_trade_date,last_rebalance_date,position_weight,
        rebalance_freq,last_net_value) VALUES ('{strategy_id}','{start_date}','','',
        {rebalance_freq},1.0)'''.format(strategy_id = strategy_id,start_date = start_date,
        rebalance_freq = rebalance_freq))
        conn.commit()
        conn.close()
        
    def save_status(self,status):           
        conn = pymssql.connect(server = '172.24.153.43',
                               user = 'DBadmin',
                               password = 'fs95536!',
                               database = 'GeniusBar')
        cursor = conn.cursor()
        cursor.execute('''
        UPDATE strategy_status SET
        last_trade_date = '{last_trade_date}',
        last_rebalance_date = '{last_rebalance_date}',
        position_weight = '{position_weight}',
        rebalance_freq = {rebalance_freq},
        last_net_value = {last_net_value}
        WHERE strategy_id = '{strategy_id}' 
        '''.format(strategy_id = status.strategy_id,
        last_trade_date = status.last_trade_date,
        last_rebalance_date = status.last_rebalance_date,
        position_weight = json.dumps(status.weight),
        rebalance_freq = status.rebalance_freq,
        last_net_value = status.last_net_value))
        
        conn.commit()
        conn.close()
        
class MVDataSource1():
    '''
    MV在研发环境中的数据源.
    '''
    def load_status(self,strategy_id):
            '''
            读取策略运行状态.
            
            Parameters
            ----------
            strategy_id
                策略id
                
            Returns
            -------
            Status
                Status对象
            '''
            status = Status(strategy_id)
            loaded_status = load_status_from_GB(strategy_id)
            
            if loaded_status is None:
                status.if_exists = False
                return status
            
            status.last_trade_date = loaded_status['last_trade_date']
            status.last_rebalance_date = loaded_status['last_rebalance_date']
            
            if len(loaded_status.status['position_weight']) == 0:
                status.weight = {}
            else:            
                status.weight = json.loads(loaded_status['position_weight'])
                
            status.rebalance_freq = loaded_status['rebalance_freq']
            status.last_net_value = float(loaded_status['last_net_value'])  
            return status


class MVSaveSource1():
    '''
    MV在研发环境中的存储源。
    '''
    pass

    

        
class BLDataSource():
    '''
    研发环境中的BL模型。
    '''
    def load_status(self,strategy_id):
        '''
        读取策略运行状态.
        
        Parameters
        ----------
        strategy_id
            策略id
            
        Returns
        -------
        Status
            Status对象
        '''
        status = Status(strategy_id)
        loaded_status = load_status_from_GB(strategy_id)
        
        if loaded_status is None:
            status.if_exists = False
            return status
        
        status.last_trade_date = loaded_status['last_trade_date']
        status.last_rebalance_date = loaded_status['last_rebalance_date']
        
        if len(loaded_status['position_weight']) == 0:
            status.weight = {}
        else:            
            status.weight = json.loads(loaded_status['position_weight'])
            
        status.rebalance_freq = loaded_status['rebalance_freq']
        status.last_net_value = float(loaded_status['last_net_value'])  
        return status
    
    def prepare_data(self,universe,start_date,end_date):
        '''
        预加载股票收盘价数据。
        
        Parameters
        ----------
        universe
            list,标的池
        start_date
            开始日期
        end_date
            结束日期
            
        Returns
        -------
        DataFrame
        '''
        return get_wsd(universe,'close',start_date,end_date)
             
    def get_daily_pct(self,universe,trade_date,prepared_data = None):
        '''
        获取universe中在交易日当天的收益率.
        
        Parameters
        ----------
        univere
            list
        trade_date
            str,YYYYMMDD
        prepared_data
            DataFrame,预加载数据
            
        Returns
        --------
        dict,key为标的代码,value为涨跌幅
        '''
        if prepared_data is not None:
            pct = prepared_data.pct_change().dropna()
            try:
                return pct.loc[dt.datetime.strptime(trade_date,'%Y%m%d').date()].to_dict()
            except KeyError:
                return None
    
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
        self.calendar = Calendar(last_trade_date,dt.datetime.today().strftime('%Y%m%d'))
        return self.calendar.trade_calendar
    
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
        return self.calendar.move(trade_date,n)
    
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
        return 0
    
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
        return get_wsd(universe,'close',start_date,end_date)  
    
    def get_wsd(self,universe,factor,start_date,end_date,names = None,if_convert = False,**options):
        return get_wsd(universe,factor,start_date,end_date,names,if_convert,**options)

    def load_P(self,customer_id,P_type):
        if P_type == 'industry':
            return pd.DataFrame([[1] + [0] * 27],columns = ['801010.SI',
                                                             '801020.SI',
                                                             '801030.SI',
                                                             '801040.SI',
                                                             '801050.SI',
                                                             '801080.SI',
                                                             '801110.SI',
                                                             '801120.SI',
                                                             '801130.SI',
                                                             '801140.SI',
                                                             '801150.SI',
                                                             '801160.SI',
                                                             '801170.SI',
                                                             '801180.SI',
                                                             '801200.SI',
                                                             '801210.SI',
                                                             '801230.SI',
                                                             '801710.SI',
                                                             '801720.SI',
                                                             '801730.SI',
                                                             '801740.SI',
                                                             '801750.SI',
                                                             '801760.SI',
                                                             '801770.SI',
                                                             '801780.SI',
                                                             '801790.SI',
                                                             '801880.SI',
                                                             '801890.SI'])
        elif P_type == 'style':
            return pd.DataFrame([[1] + [0] * 5],columns = \
                                ['399372.SZ', '399373.SZ', '399374.SZ', '399375.SZ', '399376.SZ', '399377.SZ'])

    
    def load_Q(self,customer_id,Q_type):
        if Q_type == 'industry':
            return pd.DataFrame([[0.05]])
        elif Q_type == 'style':
            return pd.DataFrame([[0.05]])

class LocalBLDataSource():
    '''
    研发环境中的本地BL模型。
    '''
    
    def __init__(self):
        self.calendar = Calendar(path = 'D:\Work\calendar')
        self.history_data = pd.read_excel('industry_data.xlsx',parse_date = 'date').set_index('date')
        self.market_cap = pd.read_excel('industry_market_cap.xlsx',parse_date = 'date').set_index('date')
        
    def load_status(self,strategy_id):
        '''
        读取策略运行状态.
        
        Parameters
        ----------
        strategy_id
            策略id
            
        Returns
        -------
        Status
            Status对象
        '''
        status = Status(strategy_id)
        loaded_status = load_status_from_GB(strategy_id)
        
        if loaded_status is None:
            status.if_exists = False
            return status
        
        status.last_trade_date = loaded_status['last_trade_date']
        status.last_rebalance_date = loaded_status['last_rebalance_date']
        
        if len(loaded_status['position_weight']) == 0:
            status.weight = {}
        else:            
            status.weight = json.loads(loaded_status['position_weight'])
            
        status.rebalance_freq = loaded_status['rebalance_freq']
        status.last_net_value = float(loaded_status['last_net_value'])  
        return status
    
    def prepare_data(self,universe,start_date,end_date):
        '''
        预加载股票收盘价数据。
        
        Parameters
        ----------
        universe
            list,标的池
        start_date
            开始日期
        end_date
            结束日期
            
        Returns
        -------
        DataFrame
        '''
        return self.history_data
             
    def get_daily_pct(self,universe,trade_date,prepared_data = None):
        '''
        获取universe中在交易日当天的收益率.
        
        Parameters
        ----------
        univere
            list
        trade_date
            str,YYYYMMDD
        prepared_data
            DataFrame,预加载数据
            
        Returns
        --------
        dict,key为标的代码,value为涨跌幅
        '''
        if prepared_data is not None:
            pct = prepared_data.pct_change().dropna()
            try:
                return pct.loc[trade_date].to_dict()
            except KeyError:
                return None
    
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
        calendar = pd.DataFrame(self.calendar.trade_calendar)
        calendar.index = calendar[0]
        return (calendar.loc[last_trade_date:dt.datetime.today().strftime('%Y%m%d')])[0].apply(lambda x:x.strftime('%Y%m%d')).values.tolist()
    
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
        return self.calendar.move(trade_date,n)
    
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
        return 0
    
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
        return self.history_data[start_date:end_date]
    
    def get_wsd(self,universe,factor,start_date,end_date,names = None,if_convert = False,**options):
        return self.market_cap.loc[start_date:end_date]

    def load_P(self,customer_id,P_type):
        if P_type == 'industry':
            return pd.DataFrame([[1] + [0] * 27],columns = ['801010.SI',
                                                             '801020.SI',
                                                             '801030.SI',
                                                             '801040.SI',
                                                             '801050.SI',
                                                             '801080.SI',
                                                             '801110.SI',
                                                             '801120.SI',
                                                             '801130.SI',
                                                             '801140.SI',
                                                             '801150.SI',
                                                             '801160.SI',
                                                             '801170.SI',
                                                             '801180.SI',
                                                             '801200.SI',
                                                             '801210.SI',
                                                             '801230.SI',
                                                             '801710.SI',
                                                             '801720.SI',
                                                             '801730.SI',
                                                             '801740.SI',
                                                             '801750.SI',
                                                             '801760.SI',
                                                             '801770.SI',
                                                             '801780.SI',
                                                             '801790.SI',
                                                             '801880.SI',
                                                             '801890.SI'])
        elif P_type == 'style':
            return pd.DataFrame([[1] + [0] * 5],columns = \
                                ['399372.SZ', '399373.SZ', '399374.SZ', '399375.SZ', '399376.SZ', '399377.SZ'])

    
    def load_Q(self,customer_id,Q_type):
        if Q_type == 'industry':
            return pd.DataFrame([[0.05]])
        elif Q_type == 'style':
            return pd.DataFrame([[0.05]])

        
def load_status_from_GB(strategy_id):
    '''
    从GeniusBar数据库读取策略状态
    
    Parameters
    -----------
    strategy_id
        策略id
        
    Returns
    -------
    dict or None
    '''
    conn = pymssql.connect(server = '172.24.153.43',
                           user = 'DBadmin',
                           password = 'fs95536!',
                           database = 'GeniusBar')
    cursor = conn.cursor()
    cursor.execute('''
    SELECT * 
    FROM strategy_status
    WHERE strategy_id = '{strategy_id}'
    '''.format(strategy_id = strategy_id))
    data = cursor.fetchall()
    
    if len(data) == 0:
        conn.close()
        return None
    else:
        columns = [each[0] for each in cursor.description]
        data_dict = pd.DataFrame(data,columns = columns).to_dict('recordes')[0]
        conn.close()
        return data_dict
