# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 14:31:14 2019

@author: ldh
"""

# tmp.py

# 获取标的过去10年的平均年化收益率

from dataflow.wind.wind_api import get_wsd

mv_universe = ['881001.WI','513500.SH','159920.SZ','518880.SH','H11025.CSI']
universe = ['881001.WI','SPX.GI','HSI.HI','882415.WI','H11025.CSI']

data = get_wsd(universe,'close','20050101','20190418')

data_pct = data.pct_change().dropna()


annual_ret = data_pct.mean() * 252
annual_ret = annual_ret.rename(index = dict(zip(universe,mv_universe)))

cov_mat = data_pct.cov() * 252
cov_mat = cov_mat.rename(index = dict(zip(universe,mv_universe)),columns = dict(zip(universe,mv_universe)))



# 准备BL模型所需要的数据


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


data = get_wsd(bl_industry_universe,'close','20100101','20190418')
data.index.name = 'date'
data.to_excel('industry_data.xlsx')


market_cap = get_wsd(bl_industry_universe,'mkt_cap_ashare','20100101','20190418')
market_cap.index.name = 'date'
market_cap.to_excel('industry_market_cap.xlsx')
