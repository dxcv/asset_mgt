# -*- coding: utf-8 -*-
"""
Created on Wed May 15 10:48:04 2019

@author: ldh
"""

# insert_into_db.py

import json
import pymssql

conn = pymssql.connect(server = '172.24.153.43',
                       user = 'DBadmin',
                       password = 'fs95536!',
                       database = 'CustData')
cursor = conn.cursor()

p1 = [0] * 28
p1[25] = 1
p1[22] = 1
p1[21] = 1
q1 = [20,20,20]

p2 = [0] * 28
p2[24] = 1
q2 = [20]

p3 = [0] * 28
q3 = [0]  

values = [(1,0,json.dumps(p1),json.dumps(q1)),
          (2,0,json.dumps(p2),json.dumps(q2)),
          (3,0,json.dumps(p3),json.dumps(q3))]

# 插入
cursor.executemany('''
               INSERT INTO BL_PQ
               (fundid,dcdate,p1,q1) VALUES (%s,%s,%s,%s)
               ''',values)
conn.commit()


# 删除
cursor.execut('''
                   DELETE FROM BL_PQ
                   WHERE fundid IN (1,2,3)
                   ''')
conn.commit()



conn = pymssql.connect(server = '172.24.153.43',
                       user = 'DBadmin',
                       password = 'fs95536!',
                       database = 'GeniusBar')
cursor = conn.cursor()
cursor.execute('''
TRUNCATE TABLE strategy_status;
TRUNCATE TABLE strategy_weight;
TRUNCATE TABLE strategy_rebalance;
TRUNCATE TABLE strategy_net_value;
                   ''')
conn.commit()
conn.close()