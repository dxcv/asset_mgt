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
    def __init__(self,strategy_id,universe,data_proxy):
        self.strategy_id = strategy_id
        self.universe = universe
        self.data_proxy
    
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

        
    def refresh(self):
        pass
    

    
