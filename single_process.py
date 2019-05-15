# -*- coding: utf-8 -*-
"""
Created on Mon May 13 13:14:51 2019

@author: ldh
"""

# single_process.py


from data_proxy import DataProxy
from data_objs import CommonDataSource,CommonSaveSource
from sim import run_sim
from strategy import MVStrategy,BLStrategy


# ----------------------------------------- MV模型 -----------------------------------------
#universe = ['881001.WI','513500.SH','159920.SZ','518880.SH','H11025.CSI']
#data_source = CommonDataSource(universe = universe,data_type = 'mv')
#save_source = CommonSaveSource()
#data_proxy = DataProxy(data_source,save_source)  
#data_proxy.pre_load()
#
#mv_strategy_ids = [('medium',0.4),('medium_high',0.7),('high',1),('medium_low',0.2),('low',0.1)]
#
#for strategy_id,risk_level in mv_strategy_ids:
#    strategy = MVStrategy(strategy_id,universe,risk_level,data_proxy)
#    run_sim(0,strategy,'20180101',data_proxy,rebalance_freq = 30)
    
# ----------------------------------------- MV模型 -----------------------------------------


# ----------------------------------------- BL行业模型 -----------------------------------------
universe = ['801010.SI',
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

cust_ids = [888800000053]

data_source = CommonDataSource(universe = universe,data_type = 'bl_industry',custs_list = cust_ids)
save_source = CommonSaveSource()
data_proxy = DataProxy(data_source,save_source)  
data_proxy.pre_load()

#mv_strategy_ids = [('medium',0.4),('medium_high',0.7),('high',1),('medium_low',0.2),('low',0.1)]


for cust_id in cust_ids:
    strategy = BLStrategy(cust_id,'bl_industry',universe,data_proxy)
    run_sim(cust_id,strategy,'20180101',data_proxy,rebalance_freq = 30)    
    
# ----------------------------------------- BL行业模型 -----------------------------------------