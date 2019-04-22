# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 17:02:46 2019

@author: ldh
"""

# main.py
import pandas as pd
from strategy import MVStrategy
from sim import run_sim

mv_universe = ['881001.WI','513500.SH','159920.SZ','518880.SH','H11025.CSI']
mv_strategy_ids = {'medium':1.0,'medium_high':1.4,'high':1.8,'medium_low':0.7,'low':0.4}

for strategy_id,risk_level in mv_strategy_ids.items():
    mv_strategy = MVStrategy(strategy_id,mv_universe,risk_level)
    rebalance_df,weight_df,net_value_df,static_indies = run_sim(mv_strategy,'20150105',30,save_path = r'D:\Work\tmp')
    
    
bl_industry_universe = []
bl_style_universe = []

