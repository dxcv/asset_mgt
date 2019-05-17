# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 16:47:14 2019

@author: ldh
"""

# utils.py
import copy
import cvxpy as cvx
import numpy as np
import pandas as pd
from cvxpy import INFEASIBLE

def optimized_weight(expected_ret,covariance_matrix,least_weight = 0.1,
                     a = None,max_sigma = None,if_consistent = False,opt_type ='ret'):
    '''
    Calculate the target weight.
    
    Parameters
    ------------
    expected_ret
        Series, the expected return of the asset.Index is the asset code.
    covariance_matrix
        DataFrame, the covariance matrix of the assets.The column and index
        is the asset code.
    least_weight
        The minimum asset weight that you wish to set.The default is 0.1.
    a
        optional,float/Series,the parameter for the objective function.Can be a Series.
    max_sigma
        The maximum sigma that can be accepted.
    if_consistent
        bool, if False, the func will make the data consistent.
    opt_type
        the optimization objective.
        'ret': to maximize the expected return given the maximum of available volatility
        'utility': to maximize the utility function
        
    Returns
    -------
    DataFrame
        Index is the asset code and the value is the weight.
        The only column is 'weight'
        
    Notes
    --------
    The index of expected_ret and covariance_matrix must be consistent.
    '''
    if not if_consistent:
        covariance_matrix = covariance_matrix.reindex(
                index = expected_ret.index,
                columns = expected_ret.index)
    
    w = cvx.Variable(len(expected_ret))
#    gamma = cvx.Parameter(sign = 'positive')
    ret = expected_ret.values.T * w
    risk = cvx.quad_form(w,covariance_matrix.values)  
    if opt_type == 'ret':
        assert max_sigma is not None
        objective = cvx.Maximize(ret)
        constraints = [cvx.sum(w) <= 1, w >= least_weight,risk <= max_sigma]

        prob = cvx.Problem(objective,constraints)
        prob.solve()
        if prob.status == INFEASIBLE:
            return {k:1/len(expected_ret) for k in expected_ret.index.tolist()}
        else:
            target_df = pd.DataFrame(w.value,
                                       index = expected_ret.index,
                                       columns = ['weight'])
            target_df.loc[target_df['weight'] < 0,'weight'] = 0
            target_dict = target_df['weight'].to_dict()
            return target_dict
#    elif opt_type == 'utility':
    #    objective = cvx.Maximize(ret - gamma * risk)
    #    constraints = [cvx.sum_entries(w) == 1, w >= 0]
#        if not isinstance(a,pd.Series):
#            gamma.value = a
#            prob.solve()        
#            return pd.DataFrame(w.value,index = expected_ret.index,
#                                columns = ['weight'])        
#        else:
#            target = pd.DataFrame()
#            for idx,val in a.iteritems():
#                gamma.value = val            
#                prob.solve()
#                tmp_ser = pd.DataFrame(w.value,index = expected_ret.index,
#                                columns = ['weight'])['weight']
#                tmp_ser.name = idx
#                target = pd.concat([target,tmp_ser],axis = 1)
#            return target
        
    

def BlackLitterman(market_weight,covariance_matrix,tau,P,Q,omega,risk_aversion,
            if_consistent = False):
    '''
    BL模型生成权重。
    
    Parameters
    ----------
    market_weight
        Series
    covariance_matrix
        DataFrame
    tau
        float,market confidence parameter
    P
        DataFrame,customer's P matrix, k * n
    Q
        Series,customer's Q matrix, k * 1
    omega
        DataFrame,confidence matrix, k * k
    risk_aversion
        float
    if_consistent
        bool,if the index and columns are consistent
        
    Returns
    ---------
    dict of weight
    '''
    assets_index = market_weight.index.tolist()
    market_weight_ser = copy.copy(market_weight)
    
    if not if_consistent:
        covariance_matrix = covariance_matrix.reindex(
                index = assets_index,
                columns = assets_index)    
        P = P.reindex(columns = assets_index)
        
    market_weight = np.mat(market_weight).T
    covariance_matrix = np.mat(covariance_matrix)
    market_expected_ret = risk_aversion * covariance_matrix * market_weight
    
    
    if isinstance(P,pd.DataFrame) and isinstance(Q,pd.DataFrame):
        P = np.mat(P)
        Q = np.mat(Q)
    assert omega.shape[0] == P.shape[0]
    assert omega.shape[1] == P.shape[0]
    
    omega = np.mat(omega)

    
    if len(P) == 0 or len(Q) == 0:
        ER = market_expected_ret
        VarianceMatrix = covariance_matrix 
    else:
        ER = ((tau * covariance_matrix).I + P.T * omega.I * P).I * \
        ((tau * covariance_matrix).I * market_expected_ret + P.T * omega.I * Q)
        
#        ER = market_expected_ret + tau * covariance_matrix  * P.T * \
#        (tau * P * covariance_matrix * P.T + omega).I * (Q - P * market_expected_ret)       
        M = ((tau * covariance_matrix).I + P.T * omega.I * P).I 
        VarianceMatrix = covariance_matrix + M
        
    try:
        w = cvx.Variable(len(ER))
        ret = ER.T * w
        risk = cvx.quad_form(w,VarianceMatrix) 
        objective = cvx.Maximize(ret - risk_aversion * risk)
        constraints = [cvx.sum(w) <= 1,w >= 0]
    
        prob = cvx.Problem(objective,constraints)
        prob.solve()
        if prob.status == INFEASIBLE:
            raise ValueError('INFEASIBLE')
        weight = pd.DataFrame(data = [np.asarray(w.value).flatten()],
                                      columns = assets_index)
        weight_dict = weight.iloc[0].to_dict()
        weight_dict_adj = {k:0 if v < 0 else v for k,v in weight_dict.items()}
        return weight_dict_adj
    except:        
        return market_weight_ser.to_dict()    


    
if __name__ == '__main__':
    pass
    # 测试MV
#    expected_ret = pd.Series(np.random.randn(30))
#    covariance_matrix = pd.DataFrame(np.eye(30))
#    a = 0.3
#    
#    weight = optimized_weight(expected_ret,covariance_matrix,max_sigma = 0.3)
    
    # 测试BL        
    from dataflow.wind.wind_api import get_wsd
    universe = \
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
    recent_ret = get_wsd(universe,'close','20160101','20160430')
    recent_ret = recent_ret.pct_change().dropna()
    hist_annual_ret = recent_ret.mean() * 252
    hist_annual_cov = recent_ret.cov() * 252
    
    
    # 市值加权
    cap_a_shares = get_wsd(universe,'mkt_cap_ashare','20160101','20160104')
    cap_weight = cap_a_shares / cap_a_shares.sum(axis = 1).iloc[0]
    market_weight = cap_weight.iloc[0]
    risk_aversion = 2.
    market_weight = np.mat(cap_weight.values[0].flatten()).T
    market_sigma = market_weight.T * np.mat(hist_annual_cov.values) * market_weight
    risk_aversion = (hist_annual_ret.mean() / market_sigma)[0,0]
    
    P = pd.DataFrame([[1] + [0] * (len(recent_ret.columns)-1)],
                      columns = recent_ret.columns.tolist())
    Q = pd.DataFrame([[20]])
    
    tau = 0.025     
    omega = pd.DataFrame(np.eye(len(P)) * 0.0009)
    weight = BlackLitterman(market_weight,hist_annual_cov,tau,P,Q,omega,risk_aversion)
    
    