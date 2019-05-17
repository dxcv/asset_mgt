# -*- coding: utf-8 -*-
"""
Created on Tue May 14 13:27:58 2019

@author: ldh
"""

# perf_parse.py
import os
import copy
import numpy as np
import pandas as pd
import json
from sqlalchemy import create_engine

def perf_parse(customer_id,strategy_id,if_plot = False):
    '''
    分析单一客户单一策略策略表现.
    '''
    engine = create_engine('mssql+pymssql://DBadmin:fs95536!@172.24.153.43/GeniusBar')
    net_value = pd.read_sql('''
                            SELECT *
                            FROM strategy_net_value
                            WHERE customer_id = {customer_id}
                            AND strategy_id = '{strategy_id}'
                            '''.format(customer_id = customer_id,strategy_id = strategy_id),
                            engine)
    net_value = net_value[['trade_date','net_value']]
    net_value.trade_date = pd.to_datetime(net_value.trade_date)
    net_value.set_index('trade_date',inplace = True)

    if if_plot:
        net_value.plot(figsize = (12,8))

    accum_ret = net_value.net_value[-1] - 1.0
    
    ## 年化收益率
    years = (net_value.index[-1] - net_value.index[0]).days / 365.
    annual_ret = np.log(1 + accum_ret) / years
    
    ## 最大回撤
    net_value_df_copy = copy.copy(net_value)
    net_value_df_copy['max_nv'] = net_value_df_copy['net_value'].cummax()
    net_value_df_copy['dd'] = (net_value_df_copy['max_nv'] - net_value_df_copy['net_value']) / net_value_df_copy['max_nv']
    max_dd = net_value_df_copy['dd'].max()
    del net_value_df_copy
    
    ## 年化收益波动率
    annual_var = net_value.net_value.pct_change().dropna().var() * 365
    
    statistic_index = pd.Series(['%.2f%%'%(accum_ret * 100),'%.2f%%'%(annual_ret * 100),'%.2f%%'%(max_dd * 100),'%.2f%%'%(annual_var * 100)],
                              index = [u'累计收益率',u'年化收益率',u'最大回撤',u'年化波动率'])
    statistic_index.name = strategy_id + '_' + str(customer_id) 
    
    return statistic_index

def perfs_parse(customer_ids,strategy_id,if_plot = False):
    '''
    分析多个客户单一策略的投资状况.
    '''
    engine = create_engine('mssql+pymssql://DBadmin:fs95536!@172.24.153.43/GeniusBar')
    net_value = pd.read_sql('''
                            SELECT *
                            FROM strategy_net_value
                            WHERE customer_id IN ({customer_ids})
                            AND strategy_id = '{strategy_id}'
                            '''.format(customer_ids = ','.join([str(each) for each in customer_ids]),
                            strategy_id = strategy_id),
                            engine)
    net_value.trade_date = pd.to_datetime(net_value.trade_date)
    net_value = net_value.pivot(index = 'trade_date',columns = 'customer_id',values = 'net_value')
    
    if if_plot:
        net_value.plot(figsize = (12,8))

    accum_ret = net_value.iloc[-1] - 1.0
    
    
    ## 年化收益率
    years = (net_value.index[-1] - net_value.index[0]).days / 365.
    annual_ret = np.log(1 + accum_ret) / years
    
    ## 最大回撤
    net_value_df_copy = copy.copy(net_value)
    max_nv = net_value_df_copy.cummax()
    dd = (max_nv - net_value_df_copy) / net_value_df_copy
    max_dd = dd.max()
    del net_value_df_copy
    
    ## 年化收益波动率
    annual_var = net_value.pct_change().dropna().var() * 365
    
    
    accum_ret.name = '累计收益率'
    annual_ret.name = '年化收益率'
    max_dd.name = '最大回撤'
    annual_var.name = '年化波动率'
    
    statistic_index = pd.concat([accum_ret * 100,annual_ret * 100,
                              max_dd * 100,annual_var * 100],axis = 1)
    statistic_index = statistic_index.applymap(lambda x:'%.2f%%'%x)
    return statistic_index

def output_strategy_perf(customer_ids,strategy_id,save_path):
    '''
    输出多个客户单个策略的表现数据。导出到指定目录。
    包括净值,持仓,调仓.
    '''    
    engine = create_engine('mssql+pymssql://DBadmin:fs95536!@172.24.153.43/GeniusBar')
    weight = pd.read_sql('''
                         SELECT * FROM strategy_weight
                         WHERE customer_id IN ({customer_ids})
                         AND strategy_id = '{strategy_id}'
                         '''.format(customer_ids = ','.join([str(each) for each in customer_ids]),
                         strategy_id = strategy_id),
                         engine)
    for cust in customer_ids:
        cust_data = weight.loc[weight['customer_id'] == cust]
        cust_data.position_weight = cust_data.position_weight.apply(lambda x :json.loads(x))
        position = pd.DataFrame.from_records(cust_data.position_weight.tolist())
        position.index = cust_data.trade_date
        position.to_excel(os.path.join(save_path,'weight_' + str(cust) + '_' + str(strategy_id) + '.xlsx'))
        
    rebalance = pd.read_sql('''
                         SELECT * FROM strategy_rebalance
                         WHERE customer_id IN ({customer_ids})
                         AND strategy_id = '{strategy_id}'
                         '''.format(customer_ids = ','.join([str(each) for each in customer_ids]),
                         strategy_id = strategy_id),
                         engine)                         
    for cust in customer_ids:
        cust_data = rebalance.loc[rebalance['customer_id'] == cust]
        cust_data.rebalance = cust_data.rebalance.apply(lambda x :json.loads(x))
        cust_rebalance = pd.DataFrame.from_records(cust_data.rebalance.tolist())
        cust_rebalance.index = cust_data.trade_date
        cust_rebalance.to_excel(os.path.join(save_path,'rebalance_' + str(cust) + '_' + str(strategy_id) + '.xlsx'))    
        
    net_value = pd.read_sql('''
                            SELECT *
                            FROM strategy_net_value
                            WHERE customer_id IN ({customer_ids})
                            AND strategy_id = '{strategy_id}'
                            '''.format(customer_ids = ','.join([str(each) for each in customer_ids]),
                            strategy_id = strategy_id),
                            engine)
                            

    net_value.trade_date = pd.to_datetime(net_value.trade_date)
    net_value = net_value.pivot(index = 'trade_date',columns = 'customer_id',values = 'net_value')
    net_value.columns = ['customer id: %s'%each for each in net_value.columns]
    net_value.to_excel(os.path.join(save_path,'net_value.xlsx'))
    
    accum_ret = net_value.iloc[-1] - 1.0
    
    
    ## 年化收益率
    years = (net_value.index[-1] - net_value.index[0]).days / 365.
    annual_ret = np.log(1 + accum_ret) / years
    
    ## 最大回撤
    net_value_df_copy = copy.copy(net_value)
    max_nv = net_value_df_copy.cummax()
    dd = (max_nv - net_value_df_copy) / net_value_df_copy
    max_dd = dd.max()
    del net_value_df_copy
    
    ## 年化收益波动率
    annual_var = net_value.pct_change().dropna().var() * 365
    
    
    accum_ret.name = '累计收益率'
    annual_ret.name = '年化收益率'
    max_dd.name = '最大回撤'
    annual_var.name = '年化波动率'
    
    statistic_index = pd.concat([accum_ret * 100,annual_ret * 100,
                              max_dd * 100,annual_var * 100],axis = 1)
    statistic_index = statistic_index.applymap(lambda x:'%.2f%%'%x)
    statistic_index.to_excel(os.path.join(save_path,'statistic_index.xlsx'))
    
    
if __name__ == '__main__':
#    customer_id = 1
#    strategy_id = 'bl_industry'
#    res = perf_parse(customer_id,strategy_id)
    
    
    customer_ids = [1,2,3]
    strategy_id = 'bl_industry'    
#    res = perfs_parse(customer_ids,strategy_id,if_plot = True)
    output_strategy_perf(customer_ids,strategy_id,'D:\\Work\\REPORTS\\20190515')
    