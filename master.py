# -*- coding: utf-8 -*-
"""
Created on Wed May  8 08:47:44 2019

@author: ldh
"""

# master.py

import os
import multiprocessing

from sim import run_sim
from data_proxy import DataProxy
from data_objs import LocalMVDataSource1,LocalMVSaveSource1
from strategy import MVStrategy
        
def MV_Simulation(task_queue,result_queue,error_queue):   
    try:
        universe = ['881001.WI','513500.SH','159920.SZ','518880.SH','H11025.CSI']
        
        data_source = LocalMVDataSource1()
        save_source = LocalMVSaveSource1('D:\\Work\\tmp\\multiprocess_save')
        data_proxy = DataProxy(data_source,save_source)  
    
        while True:
            if task_queue.empty():
                break
            task = task_queue.get(block = False,timeout = 1)
            strategy_id = task[0]
            risk_level = task[1]
            strategy = MVStrategy(strategy_id,universe,risk_level,data_proxy)
            run_sim(strategy,'20150101',data_proxy,rebalance_freq = 30)
            result_queue.put('OK for %s'%strategy_id)
            
    except Exception as e:
        error_queue.put(e)
        
def BL_Industry_Simulation(task_queue,result_queue,error_queue):   
    try:
        universe = ['881001.WI','513500.SH','159920.SZ','518880.SH','H11025.CSI']
        
        data_source = LocalMVDataSource1()
        save_source = LocalMVSaveSource1('D:\\Work\\tmp\\multiprocess_save')
        data_proxy = DataProxy(data_source,save_source)  
    
        while True:
            if task_queue.empty():
                break
            task = task_queue.get(block = False,timeout = 1)
            strategy_id = task[0]
            risk_level = task[1]
            strategy = MVStrategy(strategy_id,universe,risk_level,data_proxy)
            run_sim(strategy,'20150101',data_proxy,rebalance_freq = 30)
            result_queue.put('OK for %s'%strategy_id)
            
    except Exception as e:
        error_queue.put(e)
        
def BL_Style_Simulation(task_queue,result_queue,error_queue):   
    try:
        universe = ['881001.WI','513500.SH','159920.SZ','518880.SH','H11025.CSI']
        
        data_source = LocalMVDataSource1()
        save_source = LocalMVSaveSource1('D:\\Work\\tmp\\multiprocess_save')
        data_proxy = DataProxy(data_source,save_source)  
    
        while True:
            if task_queue.empty():
                break
            task = task_queue.get(block = False,timeout = 1)
            strategy_id = task[0]
            risk_level = task[1]
            strategy = MVStrategy(strategy_id,universe,risk_level,data_proxy)
            run_sim(strategy,'20150101',data_proxy,rebalance_freq = 30)
            result_queue.put('OK for %s'%strategy_id)
            
    except Exception as e:
        error_queue.put(e)
            
if __name__ == '__main__':
    
    mv_strategy_ids = [('medium',0.4),('medium_high',0.7),('high',1),('medium_low',0.2),('low',0.1)]
    
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    error_queue = multiprocessing.Queue()
    
    for each in mv_strategy_ids:
        task_queue.put(each)
        
    cpu_count = multiprocessing.cpu_count()
    processes_list = []
    for i in range(cpu_count):
        p_executor = multiprocessing.Process(target = MV_Simulation,args = (task_queue,result_queue,error_queue))
        p_executor.start()
        processes_list.append(p_executor)    
                
    counts = 0
    while counts < 5:
        try:
            error = error_queue.get(block = False)
            counts += 1
            print(error)
        except:
            pass
        try:
            res = result_queue.get(block = False)
            counts += 1
            print(res)
        except:
            pass
        
    for process in processes_list:
        process.join()
        process.close()
        
    os.system('pause')