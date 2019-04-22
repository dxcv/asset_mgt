# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 09:30:42 2019

@author: ldh
"""

# strategy.py

import os
import copy
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm
from dateutil.parser import parse
from data import load_status,create_trade_calendar,strategy_status_first_into_GB,\
get_daily_pct,prepare_data

def run_sim(strategy,start_date,rebalance_freq,save_path = None):
    '''
    运行资产配置策略模拟.
    
    Parameters
    ----------
    strategy
        策略对象
    start_date
        策略开始运行日期
    rebalance_freq
        调仓频率
    save_path
        结果保存路径
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
    
    calendar = create_trade_calendar(last_trade_date) 
    
    # 记录用变量
    rebalance_hist = []
    weight_hist = []
    net_value_hist = []
    
    # 回测与模拟切换
    # ---------------------------------------------------------------
#    prepared_data = prepare_data(strategy.universe,last_trade_date,
#                                 dt.datetime.today().strftime('%Y%m%d'))
    prepared_data = pd.read_excel('MVDATA.xlsx')
    # ---------------------------------------------------------------
    
    for trade_date in tqdm(calendar):                
        # 判断是否进行调仓
        if if_rebalance(rebalance_freq,last_rebalance_date,trade_date):
            
            # 修改position
            new_weight = strategy.yield_weight(trade_date) 
            
            if new_weight is None:
#                print('No Solution')
                last_rebalance_date = trade_date
                continue
            rebalance_weight = get_rebalance(weight,new_weight)
            
            # 添加调仓日期
            rebalance_weight['trade_date'] = trade_date
            rebalance_hist.append(rebalance_weight)
            weight = new_weight
            
            # 更新调仓日
            last_rebalance_date = trade_date
            
        # 将净值、持仓进行记录
        result = market_update(weight,last_net_value,trade_date,prepared_data)
        if result:
            new_net_value,weight = result
        else:
            break
        last_net_value = new_net_value
        net_value_hist.append([trade_date,new_net_value])          
        weight_to_record = copy.copy(weight)
        weight_to_record['trade_date'] = trade_date
        weight_hist.append(weight_to_record)
    
    # 更新status
    status.weight = weight
    status.last_rebalance_date = last_rebalance_date  
    status.last_trade_date = trade_date
    status.rebalance_freq = rebalance_freq 
    status.last_net_value = last_net_value
    status.save() 
    
    # 运行结果
    net_value_df = pd.DataFrame(net_value_hist,columns = ['trade_date','net_value']).set_index('trade_date')
    weight_df = pd.DataFrame.from_records(weight_hist).set_index('trade_date')
    rebalance_df = pd.DataFrame.from_records(rebalance_hist).set_index('trade_date')
    if save_path:
            net_value_df.to_excel(os.path.join(save_path,strategy.strategy_id + '_' + 'net_value.xlsx'))
            weight_df.to_excel(os.path.join(save_path,strategy.strategy_id + '_' +'weight.xlsx'))
            rebalance_df.to_excel(os.path.join(save_path,strategy.strategy_id + '_' +'rebalance.xlsx'))
 
    # --------------------------------------------------------------------------------------------------------
    # 统计指标    
    ## 累计收益率
    accum_ret = net_value_hist[-1][1] - 1.0
    
    ## 年化收益率
    years = (parse(net_value_df.index[-1]) - parse(net_value_df.index[0])).days / 365.
    annual_ret = np.log(1 + accum_ret) / years
    
    ## 最大回撤
    net_value_df_copy = copy.copy(net_value_df)
    net_value_df_copy['max_nv'] = net_value_df_copy['net_value'].cummax()
    net_value_df_copy['dd'] = (net_value_df_copy['max_nv'] - net_value_df_copy['net_value']) / net_value_df_copy['max_nv']
    max_dd = net_value_df_copy['dd'].max()
    del net_value_df_copy
    
    ## 年化收益波动率
    annual_var = net_value_df['net_value'].pct_change().dropna().var() * 365.
    
    static_indies = pd.Series([accum_ret,annual_ret,max_dd,annual_var],
                              index = [u'累计收益率',u'年化收益率',u'最大回撤',u'年化波动率'])
    static_indies.name = strategy.strategy_id
    static_indies.to_excel(os.path.join(save_path,strategy.strategy_id + '_' +'statics.xlsx'))
    # --------------------------------------------------------------------------------------------------------


    
    return rebalance_df,weight_df,net_value_df,static_indies
    
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
    rebalance_dict = {}
    
    for key,val in new_weight.items():
        if key in weight.keys():
            rebalance_weight = val - weight[key]
            if rebalance_weight == 0:
                continue
            else:
                rebalance_dict[key] = rebalance_weight
        else:
            rebalance_dict[key] = val
    
    # to sell
    to_sell = list(set(weight.keys()) - set(new_weight.keys()))
    
    for sec in to_sell:
        rebalance_dict[sec] = - weight[sec]
        
    return rebalance_dict

def if_rebalance(rebalance_freq,last_rebalance_date,trade_date):
    '''
    判断是否进行调仓.
    
    Parameters
    ----------
    rebalance_freq
        int
    last_rebalance_date
        YYYYMMDD
    trade_date
        YYYYMMDD
    '''
    if len(last_rebalance_date) == 0:
        return True
    time_diff = dt.datetime.strptime(trade_date,'%Y%m%d') - \
    dt.datetime.strptime(last_rebalance_date,'%Y%m%d')
    time_diff = int(time_diff.days)
    
    if time_diff >= rebalance_freq:
        return True
    else:
        return False
    
def market_update(weight,last_net_value,trade_date,prepared_data):
    '''
    市场更新
    
    Parameters
    ----------
    weight
        dict,当前持仓权重
    last_net_value
        上一交易日净值
    trade_date
        交易日
    prepared_data
        预加载数据
        
    Returns
    ------
    以收盘价计算的新净值与新权重
    '''
    universe = list(weight.keys())
    universe_pct = get_daily_pct(universe,trade_date,prepared_data) # dict,获取交易日当天的涨跌幅
    if universe_pct is None:
        print(trade_date)
        return None
    daily_pct = sum([universe_pct[key] * val for key,val in weight.items()])
    new_weight = {k:(1 + universe_pct[k]) * v for k,v in weight.items() }
    total_weight = sum([v for k,v in new_weight.items()])
    for k,v in new_weight.items():
        new_weight[k] = v / total_weight
    new_net_value = last_net_value * ( 1 + daily_pct)
    return new_net_value,new_weight
    


    

    

    

    
