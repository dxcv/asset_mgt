# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 13:12:11 2019

@author: ldh
"""

# core.py

import json
from data import load_status_from_GB

class Strategy():
    '''
    策略类.
    '''
    def __init__(self,strategy_id):
        self.strategy_id = strategy_id
    
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
            
        self.rebalance_freq = self.status['rebalance']
        self.last_net_value = float(self.status['last_net_value'])
        
    def refresh(self):
        pass
    
    def save(self):
        pass
    
    
class DataProxy():
    '''
    数据代理.
    '''
    def __init__(self):
        pass
    
    def prepare_data(self):
        pass