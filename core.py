# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 13:12:11 2019

@author: ldh
"""

# core.py

import json
import pymssql
import pandas as pd

class Strategy():
    '''
    策略类.
    '''
    def __init__(self,strategy_id,universe):
        self.strategy_id = strategy_id
        self.universe = universe
    
    def yield_weight(self):
        '''
        生成持仓。
        
        Returns
        --------
        dict
        '''
        pass
    
    
class Status():
    '''
    策略运行状态。
    '''
    def __init__(self,strategy_id):
        self.strategy_id = strategy_id
        self.if_exists = True
        self.last_trade_date = None
        self.last_rebalance_date = None
        self.weight = None
        self.rebalance_freq = None
        self.last_net_value = None
        
    def load_status_from_GB(self):
        '''
        从Genuis Bar数据库读取状态信息.
        '''
        self.status = load_status_from_GB(self.strategy_id)
        
        if self.status is None:
            self.if_exists = False
            return 
        
        self.last_trade_date = self.status['last_trade_date']
        self.last_rebalance_date = self.status['last_rebalance_date']
        
        if len(self.status['position_weight']) == 0:
            self.weight = {}
        else:            
            self.weight = json.loads(self.status['position_weight'])
            
        self.rebalance_freq = self.status['rebalance_freq']
        self.last_net_value = float(self.status['last_net_value'])
        
    def refresh(self):
        pass
    
    def save(self):
        conn = pymssql.connect(server = '172.24.153.43',
                               user = 'DBadmin',
                               password = 'fs95536!',
                               database = 'GeniusBar')
        cursor = conn.cursor()
#        print('''
#            UPDATE strategy_status SET
#            last_trade_date = '{last_trade_date}',
#            last_rebalance_date = '{last_rebalance_date}',
#            position_weight = '{position_weight}',
#            rebalance_freq = {rebalance_freq},
#            last_net_value = {last_net_value}
#            WHERE strategy_id = '{strategy_id}' 
#            '''.format(strategy_id = self.strategy_id,
#            last_trade_date = self.last_trade_date,
#            last_rebalance_date = self.last_rebalance_date,
#            position_weight = json.dumps(self.weight),
#            rebalance_freq = self.rebalance_freq,
#            last_net_value = self.last_net_value))
        cursor.execute('''
            UPDATE strategy_status SET
            last_trade_date = '{last_trade_date}',
            last_rebalance_date = '{last_rebalance_date}',
            position_weight = '{position_weight}',
            rebalance_freq = {rebalance_freq},
            last_net_value = {last_net_value}
            WHERE strategy_id = '{strategy_id}' 
            '''.format(strategy_id = self.strategy_id,
            last_trade_date = self.last_trade_date,
            last_rebalance_date = self.last_rebalance_date,
            position_weight = json.dumps(self.weight),
            rebalance_freq = self.rebalance_freq,
            last_net_value = self.last_net_value))
        
        conn.commit()
        conn.close()
    
        
    
class DataProxy():
    '''
    数据代理.
    '''
    def __init__(self):
        pass
    
    def prepare_data(self):
        pass
    
    
    
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