# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 17:07:37 2019

@author: ldh
"""

# report.py


import pandas as pd
from strategy import MVStrategy
from sim import run_sim

mv_universe = ['881001.WI','513500.SH','159920.SZ','518880.SH','H11025.CSI']
mv_strategy_ids = {'medium':0.4,'medium_high':0.6,'high':1,'medium_low':0.2,'low':0.1}

net_value_comb = pd.DataFrame()
statics = pd.DataFrame()

for strategy_id,risk_level in mv_strategy_ids.items():
    mv_strategy = MVStrategy(strategy_id,mv_universe,risk_level)
    rebalance_df,weight_df,net_value_df,static_indies = run_sim(mv_strategy,'20150105',30,save_path = r'D:\Work\tmp')
    net_value_df.rename(inplace = True,columns = {'net_value':strategy_id})
    net_value_comb = pd.concat([net_value_comb,net_value_df],axis = 1)
    statics = pd.concat([statics,static_indies],axis = 1)
    
net_value_comb.to_excel('D:\\Work\\tmp\\net_value_comb.xlsx')
statics.to_excel('D:\\Work\\tmp\\statics.xlsx') 


data = pd.read_excel('data.xlsx')
wind_a = data[['881001.WI']]
wind_a.index = wind_a.index.strftime('%Y%m%d')
net_value_comb1 = net_value_comb.join(wind_a,how = 'left')
net_value_comb1['881001.WI'] = net_value_comb1['881001.WI']  / net_value_comb1['881001.WI'].iloc[0]
net_value_comb1.to_excel('D:\\Work\\tmp\\net_value_comb.xlsx')
 


accum_ret = net_value_comb1['881001.WI'].iloc[-1] - 1.0

## 年化收益率
from dateutil.parser import parse
import numpy as np
import copy
years = (parse( net_value_comb1.index[-1]) - parse( net_value_comb1.index[0])).days / 365.
annual_ret = np.log(1 + accum_ret) / years

## 最大回撤
net_value_df_copy = copy.copy(net_value_comb1[['881001.WI']])
net_value_df_copy['max_nv'] = net_value_df_copy['881001.WI'].cummax()
net_value_df_copy['dd'] = (net_value_df_copy['max_nv'] - net_value_df_copy['881001.WI']) / net_value_df_copy['max_nv']
max_dd = net_value_df_copy['dd'].max()
del net_value_df_copy

## 年化收益波动率
annual_var = net_value_comb1.pct_change().dropna().var() * 365.

static_indies = pd.Series([accum_ret,annual_ret,max_dd,annual_var],
                          index = [u'累计收益率',u'年化收益率',u'最大回撤',u'年化波动率'])