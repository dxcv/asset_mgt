# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 17:02:46 2019

@author: ldh

对于回测模拟程序，由以下三个组件构成
1. 策略对象
2. 数据源对象
3. 存储源对象

在创建以上三个组件之后,数据源核存储源对象使用数据代理,通过模拟引擎sim构建.
"""

# main.py

import pandas as pd
from strategy import MVStrategy,BLStrategy
from data_proxy import DataProxy
#from data_objs import LocalMVDataSource1,LocalMVSaveSource1
from sim import run_sim

#------------------ MV模型 ------------------
#data_source = LocalMVDataSource1()
#save_source = LocalMVSaveSource1('D:\\Temp') ## TODO:存储接口最好不要在一开始定义
#data_proxy = DataProxy(data_source,save_source)
#
#mv_universe = ['881001.WI','513500.SH','159920.SZ','518880.SH','H11025.CSI']
#mv_strategy_ids = {'medium':0.4,'medium_high':0.7,'high':1,'medium_low':0.2,'low':0.1}
#
#for strategy_id,risk_level in mv_strategy_ids.items():
#    mv_strategy = MVStrategy(strategy_id,mv_universe,risk_level,data_proxy)
#    rebalance_df,weight_df,net_value_df = \
#    run_sim(mv_strategy,'20150105',data_proxy,30)
#--------------------------------------------
   
    
#------------------ BL模型 ------------------  
# 行业配置 
bl_industry_universe = \
['801010.SI',
 '801020.SI',
 '801030.SI',
 '801040.SI',
 '801050.SI',
 '801080.SI',
 '801110.SI',
 '801120.SI',
 '801130.SI',
 '801140.SI',
 '801150.SI',
 '801160.SI',
 '801170.SI',
 '801180.SI',
 '801200.SI',
 '801210.SI',
 '801230.SI',
 '801710.SI',
 '801720.SI',
 '801730.SI',
 '801740.SI',
 '801750.SI',
 '801760.SI',
 '801770.SI',
 '801780.SI',
 '801790.SI',
 '801880.SI',
 '801890.SI']


from data_objs import LocalBLDataSource,LocalMVSaveSource
data_source = LocalBLDataSource()
save_source = LocalMVSaveSource('D:\\Temp')
data_proxy = DataProxy(data_source,save_source)

bl_strategy = ['sample_customer']
for strategy_id in bl_strategy:
    bl_strategy = BLStrategy(strategy_id,bl_industry_universe,data_proxy,'industry')
    rebalance_df,weight_df,net_value_df = \
    run_sim(strategy_id,bl_strategy,'20150105',data_proxy,30)
    
# 风格配置
#bl_style_universe = ['399372.SZ', '399373.SZ', '399374.SZ', '399375.SZ', '399376.SZ', '399377.SZ']
#
#
#bl_strategy = ['sample_customer']
#for strategy_id in bl_strategy:
#    bl_strategy = BLStrategy(strategy_id,bl_style_universe,data_proxy)
#    rebalance_df,weight_df,net_value_df = \
#    run_sim(bl_strategy,'20150105',data_proxy,30)


#--------------------------------------------