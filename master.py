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
from data_objs import MVDataSource,BLDataSourceIndustry,\
BLDataSourceStyle,CommonSaveSource
from strategy import MVStrategy,BLStrategy
        

        
def run_MV():
    data_source = MVDataSource()
    save_source = CommonSaveSource()
    data_proxy = DataProxy(data_source,save_source)  
    data_proxy.pre_load()
    
    def MV_Simulation(task_queue,result_queue,error_queue):   
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
            
    mv_strategy_ids = [('medium',0.4),('medium_high',0.7),('high',1),('medium_low',0.2),('low',0.1)]
    
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    error_queue = multiprocessing.Queue()
    
    for each in mv_strategy_ids:
        task_queue.put(each)
        
    task_total_nums = task_queue.qsize()
    
    cpu_count = multiprocessing.cpu_count()
    processes_list = []
    try:
        for i in range(cpu_count):
            p_executor = multiprocessing.Process(target = MV_Simulation,
                                                 args = (task_queue,result_queue,error_queue))
            p_executor.start()
            processes_list.append(p_executor)    
                    
        while result_queue.qsize() < task_total_nums:            
            try:
                error = error_queue.get(block = False,timeout = 2)
                print(error)
            except:
                continue
        os.system('pause')
    except Exception as e:
        print(e)
        os.system('pause')
#    for p in processes_list:
#        p.join()  
    
def run_BL_industry(cust_ids):
    data_source = BLDataSourceIndustry()
    save_source = CommonSaveSource()
    data_proxy = DataProxy(data_source,save_source)
    data_proxy.pre_load()
    
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
    
    def BL_Industry_Simulation(task_queue,result_queue,error_queue):   
        try:                                
            while True:
                if task_queue.empty():
                    break
                customer_id = task_queue.get(block = False,timeout = 1)
                strategy = BLStrategy(customer_id,'bl_industry',universe,data_proxy)
                run_sim(strategy,'20180101',data_proxy,rebalance_freq = 30)
                result_queue.put('BL Industry Succeded For Customer: %s'%customer_id)                
        except Exception as e:
            error_queue.put('BL Industry Failed For Customer:%s \n Reasons:%s'%(customer_id,e))
            result_queue.put('BL Industry Failed For Customer:%s \n Reasons:%s'%(customer_id,e))
            
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    error_queue = multiprocessing.Queue()
    
    for each in cust_ids:
        task_queue.put(each)
        
    task_total_nums = task_queue.qsize()
    cpu_count = multiprocessing.cpu_count()
    processes_list = []
    for i in range(cpu_count):
        p_executor = multiprocessing.Process(target = BL_Industry_Simulation,
                                             args = (task_queue,result_queue,error_queue))
        p_executor.start()
        processes_list.append(p_executor)    
                
        
    while result_queue.qsize() < task_total_nums:            
        try:
            error = error_queue.get(block = False,timeout = 2)
            print(error)
        except:
            continue
    os.system('pause')    


def run_BL_style(cust_ids):
    data_source = BLDataSourceIndustry()
    save_source = CommonSaveSource()
    data_proxy = DataProxy(data_source,save_source)
    data_proxy.pre_load()
    
    universe = ['399372.SZ', '399373.SZ', '399374.SZ', '399375.SZ', '399376.SZ', '399377.SZ'] 
    
    def BL_Style_Simulation(task_queue,result_queue,error_queue):   
        try:
            data_source = BLDataSourceStyle()
            save_source = CommonSaveSource()
            data_proxy = DataProxy(data_source,save_source)  
        
            while True:
                if task_queue.empty():
                    break
                customer_id = task_queue.get(block = False,timeout = 1)
                strategy = BLStrategy(customer_id,'bl_style',universe,data_proxy)
                run_sim(strategy,'20180101',data_proxy,rebalance_freq = 30)
                result_queue.put('BL Style Succeded For Customer: %s'%customer_id)   
                
        except Exception as e:
            error_queue.put('BL Industry Failed For Customer:%s \n Reasons:%s'%(customer_id,e))
            result_queue.put('BL Industry Failed For Customer:%s \n Reasons:%s'%(customer_id,e))
            
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    error_queue = multiprocessing.Queue()
    
    for each in cust_ids:
        task_queue.put(each)
    task_total_nums = task_queue.qsize()        
    cpu_count = multiprocessing.cpu_count()
    processes_list = []
    for i in range(cpu_count):
        p_executor = multiprocessing.Process(target = BL_Style_Simulation,
                                             args = (task_queue,result_queue,error_queue))
        p_executor.start()
        processes_list.append(p_executor)    
                
        
    while result_queue.qsize() < task_total_nums:            
        try:
            error = error_queue.get(block = False,timeout = 2)
            print(error)
        except:
            continue
    os.system('pause')   
        
if __name__ == '__main__':
    data_source = MVDataSource()
    save_source = CommonSaveSource()
    data_proxy = DataProxy(data_source,save_source)  
    data_proxy.pre_load()
    
    from task_funcs import MV_Simulation

            
    mv_strategy_ids = [('medium',0.4),('medium_high',0.7),('high',1),('medium_low',0.2),('low',0.1)]
    
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    error_queue = multiprocessing.Queue()
    
    for each in mv_strategy_ids:
        task_queue.put(each)
        
    task_total_nums = task_queue.qsize()
    
    cpu_count = multiprocessing.cpu_count()
    processes_list = []
    try:
        for i in range(cpu_count):
            p_executor = multiprocessing.Process(target = MV_Simulation,
                                                 args = (task_queue,result_queue,error_queue,data_proxy))
            p_executor.start()
            processes_list.append(p_executor)    
                    
        while result_queue.qsize() < task_total_nums:            
            try:
                error = error_queue.get(block = False,timeout = 2)
                print(error)
            except:
                continue
        os.system('pause')
    except Exception as e:
        print(e)
        os.system('pause')

