# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 13:11:07 2019

@author: ldh
"""

# data.py



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
    calendar = get_tdays(last_trade_date,dt.datetime.today().strftime('%Y%m%d')).\
    apply(lambda x: x.strftime('%Y%m%d'))
    return calendar

def prepare_data(universe,start_date,end_date):
    '''
    预加载数据。
    
    Parameters
    ----------
    universe
        list,标的池
    start_date
        开始日期
    end_date
        结束日期
        
    Returns
    -------
    DataFrame
    '''
    return get_wsd(universe,'close',start_date,end_date)

def get_daily_pct(universe,trade_date,prepared_data = None):
    '''
    获取universe中在交易日当天的收益率.
    
    Parameters
    ----------
    univere
        list
    trade_date
        str,YYYYMMDD
    prepared_data
        DataFrame,预加载数据
        
    Returns
    --------
    dict
    '''
    if prepared_data is not None:
        pct = prepared_data.pct_change().dropna()
        try:
            return pct.loc[dt.datetime.strptime(trade_date,'%Y%m%d').date()].to_dict()
        except KeyError:
            return None
    
    
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
#    status = load_status_from_GB('asset_risk_5')
    mv_universe = ['881001.WI','513500.SH','159920.SZ','518880.SH','H11025.CSI']
    prepared_data = prepare_data(mv_universe,'20140101','20190416')
    a = get_daily_pct(mv_universe,'20190415',prepared_data)
    prepared_data.dropna().to_excel('MVDATA.xlsx')