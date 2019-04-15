# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 09:30:42 2019

@author: ldh
"""

# strategy.py

import datetime as dt

from data import load_status,create_trade_calendar,strategy_status_first_into_GB

def run_sim(strategy,start_date,rebalance_freq):
    '''
    运行大类资产配置策略模拟.
    
    Parameters
    ----------
    strategy
        策略对象
    start_date
        策略开始运行日期
    rebalance_freq
        调仓频率
    '''
    status = load_status(strategy.strategy_id) 
    
    if not status.if_exists: # 策略首次运行
        # 插入策略状态
        strategy_status_first_into_GB(strategy.strategy_id,start_date,rebalance_freq)
        status = load_status(strategy.strategy_id)      
        
    weight = status.weight
    last_rebalance_date = status.last_rebalance_date
    last_trade_date = status.last_trade_date
    rebalance_freq = status.rebalance_freq
    last_net_value = status.last_net_value
    
    calendar = create_trade_calendar(last_trade_date) # db
    
    rebalance_hist = []
    weight_hist = []
    net_value_hist = []
    
    for trade_date in calendar:                
        # 判断是否进行调仓
        if if_rebalance(rebalance_freq,last_rebalance_date,trade_date) \
        or len(last_rebalance_date) == 0:
            
            # 修改position
            new_weight = strategy.yield_weight() # db
            rebalance_weight = get_rebalance(weight,new_weight)
            
            # 添加调仓日期
            rebalance_weight['trade_date'] = trade_date
            rebalance_hist.append(rebalance_weight)
            weight = new_weight
            
            # 更新调仓日
            last_rebalance_date = trade_date
            
        # 将净值、持仓进行记录
        net_value_hist.append(get_new_value(weight,last_net_value,trade_date)) # db
        weight_hist.append(weight)
    
    # 更新status
    status.refresh()
    status.save()
    
    # 写入数据库
   
    
def get_rebalance(weight,new_weight):
    '''
    计算权重更新.
    
    Parameters
    -----------
    weight
        dict
    new_weight
        dict
    '''
    pass

def if_rebalance(rebalance_freq,last_rebalance_date,trade_date):
    '''
    判断是否进行调仓.
    '''
    pass
    
def get_new_value(weight,last_net_value,trade_date):
    '''
    返回最新净值.
    
    Parameters
    ----------
    weight
        dict,当前持仓权重
    '''
    pass


    

    

    

    
