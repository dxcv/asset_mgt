# -*- coding: utf-8 -*-
"""
Created on Mon May 13 11:28:02 2019

@author: ldh
"""

# task_funcs.py

from strategy import MVStrategy
from sim import run_sim

def MV_Simulation(task_queue,result_queue,error_queue,data_proxy):   
    try:
        universe = ['881001.WI','513500.SH','159920.SZ','518880.SH','H11025.CSI']
                
        while True:
            if task_queue.empty():
                break
            task = task_queue.get(block = False,timeout = 1)
            strategy_id = task[0]
            risk_level = task[1]
            strategy = MVStrategy(strategy_id,universe,risk_level,data_proxy)
            run_sim(strategy,'20180101',data_proxy,rebalance_freq = 30)
            result_queue.put('MV Succeded For %s'%strategy_id)
        
    except Exception as e:
        error_queue.put('MV Failed For %s \n Reasons:%s'%(strategy_id,e))
        result_queue.put('MV Failed For %s \n Reasons:%s'%(strategy_id,e))