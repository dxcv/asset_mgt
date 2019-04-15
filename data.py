# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 13:11:07 2019

@author: ldh
"""

# data.py

import pymssql
import pandas as pd
from core import Status

#%% Load
def load_status(strategy_id):
    '''
    读取策略运行状态.
    
    Parameters
    ----------
    strategy_id
        策略id
        
    Returns
    -------
    Status
        Status对象
    '''
    status = Status(strategy_id)
    status.load_status_from_GB() # 从GB数据库读取策略状态   
    return status

def load_status_from_GB(strategy_id):
    '''
    从GeniusBar数据库读取策略状态
    
    Parameters
    -----------
    strategy_id
        策略id
        
    Returns
    -------
    dict or None
    '''
    conn = pymssql.connect(server = '172.24.153.43',
                           user = 'DBadmin',
                           password = 'fs95536!',
                           database = 'GeniusBar')
    cursor = conn.cursor()
    cursor.execute('''
    SELECT * 
    FROM strategy_status
    WHERE strategy_id = '{strategy_id}'
    '''.format(strategy_id = strategy_id))
    data = cursor.fetchall()
    
    if len(data) == 0:
        conn.close()
        return None
    else:
        columns = [each[0] for each in cursor.description]
        data_dict = pd.DataFrame(data,columns = columns).to_dict('recordes')[0]
        conn.close()
        return data_dict

def create_trade_calendar(last_trade_date):
    '''
    生成交易日历.
    
    Parameters
    ---------
    last_trade_date
        上一交易日
        
    Returns
    --------
    返回上一交易日至今的交易日历，不包含上一交易日
    '''
    pass

#%% Save
def write_into_db():
    '''
    写入数据库.
    '''
    pass

def strategy_status_first_into_GB(strategy_id,start_date,rebalance_freq):
    conn = pymssql.connect(server = '172.24.153.43',
                           user = 'DBadmin',
                           password = 'fs95536!',
                           database = 'GeniusBar')
    cursor = conn.cursor()
    cursor.execute('''
       INSERT INTO strategy_status
    (strategy_id,last_trade_date,last_rebalance_date,position_weight,
    rebalance_freq,last_net_value) VALUES ('{strategy_id}','{start_date}','','',
    {rebalance_freq},1.0)'''.format(strategy_id = strategy_id,start_date = start_date,
    rebalance_freq = rebalance_freq))
    conn.commit()
    conn.close()
    
if __name__ == '__main__':
    status = load_status_from_GB('asset_risk_5')