# -*- coding: utf-8 -*-
"""
Created on Tue May  7 14:47:24 2019

@author: ldh
"""

# master.py

import time
import os
import multiprocessing


# 获取任务列表


def task_manager(task_list,task_queue):
    for task in task_list:
        task_queue.put(task)
        
def task_executor(task_queue,result_queue):
#    from .main import simulate
    import pandas as pd
    
    while True:
        task = task_queue.get()
        a = pd.DataFrame(data = [[task,3],[4,5]])
        result_queue.put(a)
#        simulate(task)
            


if __name__ == '__main__':
    customer_list = ['cust_1','cust_2','cust_3','cust_4','cust_5','cust_6']
    
    task_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    
    p_manager = multiprocessing.Process(target = task_manager,args = (customer_list,task_queue))
    p_manager.start()
    p_manager.join()
    p_manager.close()
    
    cpu_count = multiprocessing.cpu_count()
    processes_list = []
    for _ in range(cpu_count):
        p_executor = multiprocessing.Process(target = task_executor,args = (task_queue,result_queue))
        p_executor.start()
        processes_list.append(p_executor)
        
    result_list = []
    while True:
        if len(result_list) == 6:
            break
        try:
            result_list.append(result_queue.get(block = False,timeout = 1))
        except:
            continue
        
    for each in result_list:
        print(each)
        
    for p in processes_list:
        p.terminate()
        p.join()
                
    os.system('pause')