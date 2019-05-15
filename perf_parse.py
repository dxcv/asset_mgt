# -*- coding: utf-8 -*-
"""
Created on Tue May 14 13:27:58 2019

@author: ldh
"""

# perf_parse.py
import copy
import pandas as pd
from sqlalchemy import create_engine

def perf_parse(customer_id,strategy_id):
    '''
    分析策略表现.
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
    
    static_indies = pd.Series(['%.2f%%'%(accum_ret * 100),'%.2f%%'%(annual_ret * 100),'%.2f%%'%(max_dd * 100),'%.2f%%'%(annual_var * 100)],
                              index = [u'累计收益率',u'年化收益率',u'最大回撤',u'年化波动率'])
    static_indies.name = strategy_id + '_' + str(customer_id) 
    
    return static_indies

if __name__ == '__main__':
    customer_id = 888800000053
    strategy_id = 'bl_industry'
    res = perf_parse(customer_id,strategy_id)
    