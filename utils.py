# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 16:47:14 2019

@author: ldh
"""

# utils.py
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
        constraints = [cvx.sum(w) == 1, w >= least_weight,risk <= max_sigma]

        prob = cvx.Problem(objective,constraints)
        prob.solve()
        if prob.status == INFEASIBLE:
#            print('INFEASIBLE')
            return None
        else:
            target_dict = pd.DataFrame(w.value,
                                       index = expected_ret.index,
                                       columns = ['weight'])['weight'].to_dict()
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
        
    
def BLModel(market_expected_ret,covariance_matrix,tau,P,Q,omega,risk_aversion,
            if_consistent = False):
    '''
    BL模型生成权重。
    
    Parameters
    ----------
    market_expected_ret
        Series
    covariance_matrix
        DataFrame
    tau
        float,market confidence parameter
    P
        DataFrame,customer's P matrix
    Q
        Series,customer's Q matrix
    omega
        DataFrame,confidence matrix
    risk_aversion
        float
    if_consistent
        bool,是否一致
        
    Returns
    ---------
    dict of weight
    '''
    
    index_names = market_expected_ret.index.tolist()
    if not if_consistent:
        covariance_matrix = covariance_matrix.reindex(
                index = market_expected_ret.index,
                columns = market_expected_ret.index)    
        P = P.reindex(columns = market_expected_ret.index)
#        Q = Q.reindex(columns = market_expected_ret.index)
        
    market_expected_ret = np.mat(market_expected_ret).T
    covariance_matrix = np.mat(covariance_matrix)
    if isinstance(P,pd.DataFrame) and isinstance(Q,pd.DataFrame):
        P = np.mat(P)
        Q = np.mat(Q)
    else:
        P = np.mat(np.zeros(1,len(market_expected_ret)))
        Q = np.mat(np.zeros(1,1))

    omega = tau * P * covariance_matrix * P.T
#    A = 1 / tau * covariance_matrix.I + P.T * omega.I * P
#    B = 1 / tau * covariance_matrix.I * market_expected_ret + P.T * omega.I * Q     
    
    ER = market_expected_ret + tau * covariance_matrix  * P.T * \
    (tau * P * covariance_matrix * P.T + omega).I * (Q - P * market_expected_ret)
    M = tau * covariance_matrix - tau * covariance_matrix * P.T * \
    (P * covariance_matrix * P.T + omega).I * P * tau * covariance_matrix
    
    VarianceMatrix = covariance_matrix + M
#    print(ER)
#    print(VarianceMatrix) 
#    print(omega)
    try:
        w = cvx.Variable(len(ER))
        ret = ER.T * w
        risk = cvx.quad_form(w,VarianceMatrix) 
        objective = cvx.Maximize(ret - risk_aversion * risk)
        constraints = [cvx.sum(w) == 1,w >= 0]
    
        prob = cvx.Problem(objective,constraints)
        prob.solve()
        if prob.status == INFEASIBLE:
            return None
        else:            
            print('Sucessfully!')
            weight = pd.DataFrame(data = [np.asarray(w.value).flatten()],columns = index_names)
            return weight.iloc[0].to_dict()
    except:
        weight = (risk_aversion * VarianceMatrix).I * ER
        weight_adj = np.asarray(weight).flatten()
        weight_abs_sum = 0
        for w in weight_adj:
            weight_abs_sum += abs(w)
        
        weight_adj = weight_adj / weight_abs_sum
        weight_adj = np.exp(weight_adj)
        weight_sum = weight_adj.sum()
        weight_adj = weight_adj / weight_sum
        
        return pd.DataFrame([weight_adj],columns = index_names).iloc[0].to_dict()
    
#    if solver_type == 'math':
#        # 数值解
#        return (risk_aversion * VarianceMatrix).I * ER
#    
#    elif solver_type == 'cvx':
#        # cvx求解
#        w = cvx.Variable(len(ER))
#        ret = ER.T * w
#        risk = cvx.quad_form(w,VarianceMatrix) 
#        objective = cvx.Maximize(ret - risk_aversion * risk)
#        constraints = [cvx.sum(w) == 1,w >= 0]
#    
#        prob = cvx.Problem(objective,constraints)
#        prob.solve()
#        if prob.status == INFEASIBLE:
#            return None
#        else:
#            return w.value
    #        target_dict = pd.DataFrame(w.value,
    #                                   index = expected_ret.index,
    #                                   columns = ['weight'])['weight'].to_dict()        

if __name__ == '__main__':
    # 测试MV
#    expected_ret = pd.Series(np.random.randn(30))
#    covariance_matrix = pd.DataFrame(np.eye(30))
#    a = 0.3
#    
#    weight = optimized_weight(expected_ret,covariance_matrix,max_sigma = 0.3)
    
    # 测试BL
#    market_expected_ret = pd.Series([0.02,0.04,-0.01,0.1],index = ['A','B','C','D'])
#    covariance_matrix = pd.DataFrame([[0.034,0.02,0.01,-0.07],
#                                      [0.02,0.054,-0.012,-0.027],
#                                      [0.01,-0.012,0.79,-0.07],
#                                      [-0.07,-0.027,-0.07,0.02]],
#            index =  ['A','B','C','D'],columns =  ['A','B','C','D'])
#    tau = 0.025
#    risk_aversion = 12
    P = pd.DataFrame([[1,0,0,0]],columns = ['A','B','C','D'])
    Q = pd.DataFrame([[0.2]])
#    omega = pd.DataFrame([[0.92**2]])
#    
#    
#    weight = BLModel(market_expected_ret,covariance_matrix,tau,P,Q,omega,risk_aversion,
#                     solver_type = 'cvx')
        
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
    expected_ret = recent_ret.mean() *  252
    expected_cov = recent_ret.cov() * 252
    
    # 等权
    market_weight = np.mat(np.ones((len(universe),1)))
    
    # 市值加权
    cap_a_shares = get_wsd(universe,'mkt_cap_ashare','20160101','20160104')
    cap_weight = cap_a_shares / cap_a_shares.sum(axis = 1).iloc[0]
    
    market_weight = np.mat(cap_weight.values[0].flatten()).T
    market_sigma = market_weight.T * np.mat(expected_cov.values) * market_weight
    risk_aversion = (expected_ret.mean() / market_sigma)[0,0]
    
    P = pd.DataFrame([[1] + [0] * (len(recent_ret.columns)-1)],
                      columns = recent_ret.columns.tolist())
    Q = pd.DataFrame([[0.2]])
    
    tau = 0.25     
    omega = 0
    weight = BLModel(expected_ret,expected_cov,tau,P,Q,omega,risk_aversion)
    
    