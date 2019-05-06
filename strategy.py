# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 13:15:07 2019

@author: ldh
"""

# strategy.py

import datetime as dt
import dateutils
import numpy as np
import pandas as pd
from core import Strategy
#from dataflow.wind.wind_api import get_wsd,get_tdaysoffset # 模拟用
from dataflow.wind.calendar import Calendar

from utils import optimized_weight,BLModel

history_data = pd.read_excel('MVDATA.xlsx')

annual_ret = pd.read_excel('.\\annual_ret.xlsx')[0]
#sigma = pd.read_excel('60sigma.xlsx')
#calendar2 = Calendar()

class MVStrategy(Strategy):
    
    def __init__(self,strategy_id,universe,risk_level,data_proxy):
        self.strategy_id = strategy_id
        self.universe = universe
        self.risk_level = risk_level
        self.data_proxy = data_proxy
        
        
    def yield_weight(self,trade_date):
        # 获取最近一段时间A股的波动率
        
        ## 取得前一个交易日的函数,在回测情况下采用历史数据
        
        pre_trade_date = self.data_proxy.move_trade_date(trade_date,-1)

        pre_date_str = pre_trade_date.strftime('%Y%m%d')
        start_date = pre_trade_date - dateutils.relativedelta(years = 1)
        start_date_str = start_date.strftime('%Y%m%d')
        
        # 全A波动率
        # ------------------------ ------------------------
#        volatility = get_wsd(["881001.WI"],"stdevry", start_date_str, pre_date_str,
#                             period = '3',returnType = '1')
#        volatility = volatility.iloc[-1]['STDEVRY'] / 100. 
        # ------------------------ ------------------------      
        
        # 沪深300波动率
        # ------------------------ ------------------------
        hs300 = self.data_proxy.get_daily_quote_index('000300.SH',start_date_str,pre_date_str)
        hs300 = hs300[['TradeDate','Close']].set_index('TradeDate')
        hs_pct = hs300.pct_change().dropna()
        volatility = (hs_pct.var() * 252)['Close']
        
        # ------------------------ ------------------------        
        
        # MV模型生成权重
        # 此处数据需要切换,考虑到流量的问题,采用本地化数据
        # -------------------------------- -------------------------------- 
        data_start_date = pre_trade_date - dateutils.relativedelta(months = 3)
        data_avl  = self.data_proxy.get_history_close_data(self.universe,
                                                         data_start_date,pre_trade_date)
        data_pct = data_avl.pct_change().dropna()
        # -------------------------------- -------------------------------- 
        
        
        # Expected Return

        expected_ret = 0.7 * data_pct.mean() * 252 + 0.3 * annual_ret
        
        
        # Expected Covariance Matrix
        covariance_matrix = data_pct.cov() * 252
        weight = optimized_weight(expected_ret,covariance_matrix,
                                  max_sigma = self.risk_level * volatility)
        return weight
    
    

class BLStrategy(Strategy):
    def __init__(self,strategy_id,universe,data_proxy,bl_type):
        self.strategy_id = strategy_id
        self.universe = universe
        self.data_proxy = data_proxy
        self.bl_type = bl_type
        
    def yield_weight(self,trade_date):
        '''
        Yield Weight.
        '''
        pre_trade_date = self.data_proxy.move_trade_date(trade_date,-1)

        pre_date_str = pre_trade_date.strftime('%Y%m%d')
        start_date = pre_trade_date - dateutils.relativedelta(months = 3)
        start_date_str = start_date.strftime('%Y%m%d')        
        
        recent_ret = self.data_proxy.get_history_close_data(self.universe,start_date_str,
                                                            pre_date_str)
        recent_ret = recent_ret.pct_change().dropna()
        expected_ret = recent_ret.mean() *  252
        expected_cov = recent_ret.cov() * 252
        
        # 等权
#        market_weight = np.mat(np.ones((len(self.universe),1)))
        
        # 市值加权
        cap_a_shares = self.data_proxy.get_wsd(self.universe,'mkt_cap_ashare',pre_date_str,pre_date_str)
        cap_weight = cap_a_shares / cap_a_shares.sum(axis = 1).iloc[0]
        
        market_weight = np.mat(cap_weight.values[0].flatten()).T
        market_sigma = market_weight.T * np.mat(expected_cov.values) * market_weight
        
        market_sigma = market_weight.T * np.mat(expected_cov.values) * market_weight
        risk_aversion = (expected_ret.mean() / market_sigma)[0,0]
        
        P = self.data_proxy.load_P(self.strategy_id,self.bl_type)
        Q = self.data_proxy.load_Q(self.strategy_id,self.bl_type)
        tau = 0.025        
        omega = 0
        weight = BLModel(expected_ret,expected_cov,tau,P,Q,omega,risk_aversion)
        return weight
        
    
