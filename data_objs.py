# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 11:26:02 2019

@author: ldh
"""

# data_objs.py
import os
import datetime as dt
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from sqlalchemy.types import TEXT,VARCHAR,DECIMAL,BIGINT
import pymssql
import json
import numpy as np
import pandas as pd
from core import Status,DataSource
from dataflow.api_dailyquote import get_daily_quote_index
from dataflow.api_calendar import Calendar


def load_status_from_GB(customer_id,strategy_id):
    '''
    从GeniusBar数据库读取策略状态
    
    Parameters
    -----------
    customer_id
        客户id
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
    WHERE customer_id = '{customer_id}'
    AND strategy_id = '{strategy_id}'
    '''.format(
    customer_id = customer_id,
    strategy_id = strategy_id))
    data = cursor.fetchall()
    
    if len(data) == 0:
        conn.close()
        return None
    else:
        columns = [each[0] for each in cursor.description]
        data_dict = pd.DataFrame(data,columns = columns).to_dict('recordes')[0]
        conn.close()
        return data_dict
        
class CommonSaveSource():
    '''
    通用存储源.
    '''   
    # 数据输出
    def write_into_db(self,customer_id,strategy_id,weight_hist,weight_dates,
                      rebalance_hist,rebalance_dates,net_value_record):
        '''
        写入数据库.
        '''
        
        weight_df = pd.DataFrame(list(zip(weight_dates,
                                     [json.dumps(weight) for weight in weight_hist])),
                                  columns = ['trade_date','position_weight'])
        rebalance_df = pd.DataFrame(list(zip(rebalance_dates,
                                             [json.dumps(rebalance) for rebalance in rebalance_hist])),
        columns = ['trade_date','rebalance'])
        net_value_record['customer_id'] = customer_id
        weight_df['customer_id'] = customer_id
        rebalance_df['customer_id'] = customer_id
        
        net_value_record['strategy_id'] = strategy_id
        weight_df['strategy_id'] = strategy_id
        rebalance_df['strategy_id'] = strategy_id
        
        conn_engine = create_engine('mssql+pymssql://DBadmin:fs95536!@172.24.153.43/GeniusBar')
        
        net_value_record.reset_index().to_sql('strategy_net_value',conn_engine,if_exists = 'append',
                                index = False,dtype = {'customer_id':BIGINT,
                                                        'strategy_id':VARCHAR(32),
                                                        'trade_date':VARCHAR(32),
                                                        'net_value':DECIMAL(20,6)})
        weight_df.to_sql('strategy_weight',conn_engine,if_exists = 'append',
                             index = False,dtype = {'customer_id':BIGINT,
                                                     'strategy_id':VARCHAR(32),
                                                     'trade_date':VARCHAR(32),
                                                     'position_weight':TEXT})
        rebalance_df.to_sql('strategy_rebalance',conn_engine,if_exists = 'append',
                                index = False,dtype = {'customer_id':BIGINT,
                                                     'strategy_id':VARCHAR(32),
                                                     'trade_date':VARCHAR(32),
                                                     'rebalance':TEXT})
    
    def strategy_status_first_into_DB(self,customer_id,strategy_id,start_date,rebalance_freq):    
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
        (customer_id,strategy_id,last_trade_date,last_rebalance_date,position_weight,
        rebalance_freq,last_net_value) VALUES ('{customer_id}','{strategy_id}','{start_date}','','',
        {rebalance_freq},1.0)'''.format(customer_id = customer_id,strategy_id = strategy_id,
        start_date = start_date,rebalance_freq = rebalance_freq))
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
        WHERE customer_id = '{customer_id}'
        AND strategy_id = '{strategy_id}'
        '''.format(customer_id = status.customer_id,
        strategy_id = status.strategy_id,
        last_trade_date = status.last_trade_date,
        last_rebalance_date = status.last_rebalance_date,
        position_weight = json.dumps(status.weight),
        rebalance_freq = status.rebalance_freq,
        last_net_value = status.last_net_value))
        
        conn.commit()
        conn.close()



class CommonDataSource(DataSource):
    '''
    BL行业数据源.
    '''
    def __init__(self,universe,data_type,custs_list = None):
        self.universe = universe
        self.data_type = data_type
        self.custs_list = custs_list
        
    def pre_load(self,n_years):
        '''
        预加载最近两年的数据.
        '''
        
        engine = create_engine('mssql+pymssql://DBadmin:fs95536!@172.24.153.43/GeniusBar')
        today = dt.datetime.today()
        years_ago = today - relativedelta(years = n_years)
        
        data = pd.read_sql('''
                             SELECT * FROM asset_data
                             WHERE asset_code IN
                             ({universe}) AND
                             trade_date >= {start_date} AND
                             trade_date <= {end_date}
                           '''.format(universe = ','.join(["'" + each + "'" for each in self.universe]),
          start_date = years_ago.strftime('%Y%m%d'),
          end_date = dt.datetime.today().strftime('%Y%m%d')),engine)
        data['trade_date'] = pd.to_datetime(data['trade_date'])
        
        # ----------------- 系统用数据 -----------------
        data_adj = data.drop('market_cap',axis = 1)
        data_adj.close_price = data_adj.close_price.astype(float)
        data_adj = pd.pivot_table(data_adj,columns = 'asset_code',values = 'close_price',index = 'trade_date')
        pct = data_adj.pct_change().dropna()
        
        calendar = Calendar('20000104',today.strftime('%Y%m%d'))
        
        self.calendar = calendar
        self.pct = pct
        self.data = data_adj
        
        
        # ----------------- 策略用数据 -----------------
        
        if self.data_type == 'mv':
            self.hs300 = get_daily_quote_index('000300.SH',years_ago.strftime('%Y%m%d'),
                                   today.strftime('%Y%m%d'))
            self.annual_ret = pd.read_excel('.//data//annual_ret.xlsx')[0]
            self.annual_cov = pd.read_excel('.//data//cov_mat.xlsx')
        elif self.data_type == 'bl_industry':
            market_cap = data.drop('close_price',axis = 1)
            market_cap.market_cap = market_cap.market_cap.astype(float)
            market_cap = pd.pivot_table(market_cap,columns = 'asset_code',values = 'market_cap',index = 'trade_date')
            market_cap.index = pd.to_datetime(market_cap.index)
            self.market_cap_ashare = market_cap
            self.PQ,self.P_column = self._load_PQ()
            
        elif self.data_type == 'bl_style':
            market_cap = data.drop('close_price',axis = 1)
            market_cap.market_cap = market_cap.market_cap.astype(float)
            market_cap = pd.pivot_table(market_cap,columns = 'asset_code',values = 'market_cap',index = 'trade_date')
            market_cap.index = pd.to_datetime(market_cap.index)
            self.market_cap_ashare = market_cap
            self.PQ = self._load_PQ(self.custs_list)      
            
    def load_status(self,customer_id,strategy_id):
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
        status = Status(customer_id,strategy_id)
        loaded_status = load_status_from_GB(customer_id,strategy_id)
        
        if loaded_status is None:
            status.if_exists = False
            return status
        
        status.customer_id = loaded_status['customer_id']
        status.last_trade_date = loaded_status['last_trade_date']
        status.last_rebalance_date = loaded_status['last_rebalance_date']
        
        if len(loaded_status['position_weight']) == 0:
            status.weight = {}
        else:            
            status.weight = json.loads(loaded_status['position_weight'])
            
        status.rebalance_freq = loaded_status['rebalance_freq']
        status.last_net_value = float(loaded_status['last_net_value'])  
        return status
        
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
            try:
                if isinstance(trade_date,str):
                    return self.pct.loc[dt.datetime.strptime(trade_date,'%Y%m%d').date()].to_dict()
                elif isinstance(trade_date,pd.Timestamp):
                    return self.pct.loc[trade_date.date()].to_dict()                
            except KeyError:
                return None     
            
    def get_market_cap_ashare(self,universe,start_date,end_date):
        return (self.market_cap_ashare[universe]).loc[start_date:end_date]
    
    def _load_PQ(self):
        conn = pymssql.connect(server = '172.24.153.43',
                           user = 'DBadmin',
                           password = 'fs95536!',
                           database = 'CustData')
        cursor = conn.cursor()
        cursor.execute('''
                       SELECT *
                       FROM BL_PQ
                       WHERE fundid IN
                       ({custs_list})
                       '''.format(custs_list = ','.join([str(each) for each in self.custs_list])))
        data = cursor.fetchall()
        columns = [each[0] for each in cursor.description]
        data = pd.DataFrame(data,columns = columns)
        
        
        cursor.execute('''
                       SELECT * 
                       FROM BL_PQ_Industry_Sequence
                       ''')
        p_columns = cursor.fetchall()
        p_columns = pd.DataFrame(p_columns,columns = [each[0] for each in cursor.description])
        p_columns.sort_values('Sqc',ascending = True,inplace = True)
        conn.close()
        return data,p_columns
        
    def load_P(self,customer_id,strategy_id):
        PQ = self.PQ.loc[self.PQ['fundid'] == customer_id]
        P = PQ['p1'].iloc[0]
        P = np.array(json.loads(P))
        P = np.eye(28)[P == 1]
        P = pd.DataFrame(P,columns = self.P_column['IndCode'].tolist())
        return P
    
    def load_Q(self,customer_id,strategy_id):
        PQ = self.PQ.loc[self.PQ['fundid'] == customer_id]
        Q = PQ['q1'].iloc[0]
        Q = np.array(json.loads(Q))
        Q = Q.reshape((len(Q),1))
        Q = pd.DataFrame(Q)
        return Q
        
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
        return (self.data[self.universe]).loc[start_date:end_date]         
        
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
        return self.hs300.loc[(self.hs300['TradeDate'] >= start_date) & (self.hs300['TradeDate'] <= end_date)]

    def get_annual_ret(self):
        return self.annual_ret
    
    def get_cov_mat(self):
        return self.annual_cov