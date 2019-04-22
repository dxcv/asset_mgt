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
from dataflow.api_dailyquote import get_daily_quote_index
from dataflow.wind.calendar import Calendar
from utils import optimized_weight,BLModel

history_data = pd.read_excel('MVDATA.xlsx')
calendar = Calendar(path = 'D:\Work\calendar')
annual_ret = pd.read_excel('D:\\Work\\asset_mgt\\annual_ret.xlsx')[0]
#sigma = pd.read_excel('60sigma.xlsx')
#calendar2 = Calendar()

class MVStrategy(Strategy):
    
    def __init__(self,strategy_id,universe,risk_level):
        self.strategy_id = strategy_id
        self.universe = universe
        self.risk_level = risk_level
        
    def yield_weight(self,trade_date):
        # 获取最近一段时间A股的波动率
        
        ## 取得前一个交易日的函数,在回测情况下采用历史数据
        
        # 回测用代码
        # ------------------------ ------------------------
#        print(trade_date)
        pre_trade_date = calendar.move(trade_date,-1)
        
        # ------------------------ ------------------------
        
        # 模拟用代码
        # ------------------------ ------------------------
#        pre_trade_date = get_tdaysoffset(-1,trade_date[:4] + \
#                                         '-' + trade_date[4:6] + '-' + trade_date[6:] )
        # ------------------------ ------------------------
        
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
        hs300 = get_daily_quote_index('000300.SH',start_date_str,pre_date_str)
        hs300 = hs300[['TradeDate','Close']].set_index('TradeDate')
        hs_pct = hs300.pct_change().dropna()
        volatility = (hs_pct.var() * 252)['Close']
        
        # ------------------------ ------------------------        
        
        # MV模型生成权重
        # 此处数据需要切换,考虑到流量的问题,采用本地化数据
        # -------------------------------- -------------------------------- 
        data_start_date = pre_trade_date - dateutils.relativedelta(months = 3)
        data_avl = history_data.loc[data_start_date:pre_trade_date]
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
    def __init__(self,strategy_id,universe):
        self.strategy_id = strategy_id
        self.universe = universe

        
    def yield_weight(self,trade_date,P,Q):
        '''
        Yield Weight.
        '''
        return BLModel()
    
