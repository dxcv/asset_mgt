# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 09:30:42 2019

@author: ldh
"""

# strategy.py


import copy
import numpy as np
import pandas as pd
import datetime as dt
from tqdm import tqdm
from dateutil.parser import parse

def run_sim(customer_id,strategy,start_date,data_proxy,rebalance_freq = 30,if_save = True,
            if_summarise = False):
    '''
    运行资产配置策略模拟.
    
    Parameters
    ----------
    customer_id
        客户id
    strategy
        策略对象
    start_date
        策略开始运行日期
    data_proxy
        数据代理
    rebalance_freq
        调仓频率，单位为自然日,回测或首次运行需要输入该参数
    if_save
        bool,是否保存
    if_summarise
        bool,是否输出统计结果
    '''
    status = data_proxy.load_status(customer_id,strategy.strategy_id) 
    
    if not status.if_exists: # 策略首次运行
        # 插入策略状态
        data_proxy.strategy_status_first_into_DB(customer_id,
                                                 strategy.strategy_id,
                                                 start_date,rebalance_freq)
        status = data_proxy.load_status(customer_id,strategy.strategy_id)      
        
    weight = status.weight
    last_rebalance_date = status.last_rebalance_date
    last_trade_date = status.last_trade_date
    rebalance_freq = status.rebalance_freq
    last_net_value = status.last_net_value
    
    calendar = data_proxy.create_trade_calendar(last_trade_date) # 不含last_trade_date 
    if last_trade_date in calendar.values:
        calendar = calendar.iloc[1:]
    
    # 记录用变量
    rebalance_hist = []
    weight_hist = []
    net_value_hist = []
    
    # 统一接口
    prepared_data = data_proxy.prepare_data(strategy.universe,last_trade_date,
                                            dt.datetime.today().strftime('%Y%m%d'))
    
    if prepared_data.shape[0] <= 1:
        return 0
    
    for trade_date in tqdm(calendar):                
        # 判断是否进行调仓
        if if_rebalance(rebalance_freq,last_rebalance_date,trade_date):
            
            # 修改position
            new_weight = strategy.yield_weight(trade_date) 
            
            if new_weight is None:
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
        result = market_update(weight,last_net_value,trade_date,data_proxy,prepared_data)
        if result:
            new_net_value,weight = result
            end_date = trade_date
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
    status.last_trade_date = end_date
    status.rebalance_freq = rebalance_freq 
    status.last_net_value = last_net_value
    data_proxy.save_status(status)
    
    # 运行结果
    net_value_df = pd.DataFrame(net_value_hist,columns = ['trade_date','net_value']).set_index('trade_date')
    weight_df = pd.DataFrame.from_records(weight_hist).set_index('trade_date')
    rebalance_df = pd.DataFrame.from_records(rebalance_hist).set_index('trade_date')
    
    if if_save:
        data_proxy.write_into_db(customer_id,strategy.strategy_id,weight_df,rebalance_df,net_value_df)
        
    if if_summarise:
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
        # --------------------------------------------------------------------------------------------------------
    
        return rebalance_df,weight_df,net_value_df,static_indies
    else:
        return rebalance_df,weight_df,net_value_df
    
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
    
def market_update(weight,last_net_value,trade_date,data_proxy,prepared_data):
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
    data_proxy
        数据代理
    prepared_data
        预加载数据
        
    Returns
    ------
    以收盘价计算的新净值与新权重
    '''
    universe = list(weight.keys())
    universe_pct = data_proxy.get_daily_pct(universe,trade_date,prepared_data) # dict,获取交易日当天的涨跌幅
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
    


    

    

    

    
