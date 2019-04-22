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
