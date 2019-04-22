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
            target_dict = pd.DataFrame(w.value,index = expected_ret.index,columns = ['weight'])['weight'].to_dict()
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
        
    
def BLModel(market_expected_ret,covariance_matrix,tau,P,Q,omega,risk_aversion):
    A = 1 / tau * covariance_matrix.I + P.T * omega.I * P
    B = 1 / tau * covariance_matrix.I * market_expected_ret + P.T * omega.I * Q    
    return 1 / risk_aversion * (covariance_matrix + A.I).I * A.I * B

if __name__ == '__main__':
    expected_ret = pd.Series(np.random.randn(30))
    covariance_matrix = pd.DataFrame(np.eye(30))
    a = 0.3
    
    weight = optimized_weight(expected_ret,covariance_matrix,max_sigma = 0.3)