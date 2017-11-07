# -*- coding: utf-8 -*-
import json,datetime
import pandas as pd
from calendar import monthrange
from collections import OrderedDict

fp = '/Users/xuegeng/Spider_Workspace/crawler/shop_datas.json'

with open(fp,'r') as f:
    data = json.load(f)
#l = [[u'2015-00-31', 0.0], [u'2015-00-30', 0.0], [u'2015-00-31', 0.0], [u'2015-00-31', 0.0], [u'2015-00-30', 0.0], [u'2015-00-31', 0.0], [u'2015-00-30', 0.0], [u'2015-00-31', 0.0], [u'2016-00-31', 0.0], [u'2016-00-29', 0.0], [u'2016-00-31', 0.0], [u'2016-00-30', 2.0], [u'2016-00-31', 0.0], [u'2016-00-30', 0.0], [u'2016-00-31', 0.0], [u'2016-00-31', 0.0], [u'2016-00-30', 0.0], [u'2016-00-31', 0.0], [u'2016-00-30', 0.0], [u'2016-00-31', 0.0], [u'2017-00-31', 0.0], [u'2017-00-28', 0.0],[u'2017-00-31', 0.2857142857142857]]
#al = [[u'2011-00-31', 0.825],[u'2012-00-31', 2.727272727272727],[u'2013-00-31', 0.37777777777777777],[u'2014-00-31', 0.9803921568627452],[u'2017-00-31', 1.5]]
 
def change_date(l):
    '''
    #长度为0
    #长度不为0
        #差距为0
            #是17年
                #取长度
                #range（1，5）后截长度
            #不是17年
                #取长度
                #range（1，13）后截长度
        #差距不为0
            #得到长度
            #统计首年尾年长度
            #range（1，13）取各年长度拼接'''
    if len(l) == 0:return []
    l2 = map(lambda x:x[0][:4],l)
    d = OrderedDict(sorted((x,l2.count(x)) for x in set(l2)))
    months = []
    #if len(d) == 1 and d.keys()[0df] != u'2017':return []
    for k,v in d.iteritems():
        if k == '2017':
            months.extend(range(1,5)[:v])
        else:
            months.extend(range(1,13)[-v:])
    months = map(lambda x:str(x) if x>9 else '0'+str(x),months)
    lb = []
    for i in range(len(months)):
        date_string = l[i][0].replace('-00','-'+months[i])
        n,y,r = date_string.split('-')
        r = str(monthrange(int(n),int(y))[1])
        date_string = '-'.join([n,y,r])
        lb.append([date_string,l[i][1]])
    return lb
annual_lambda = lambda y:map(lambda x:[x[0].replace('-00','-12'),x[1]],y)







df = pd.DataFrame(data)
df.monthly_comm_dense = df.monthly_comm_dense.apply(change_date)
df.annual_com_per_growth = df.annual_com_per_growth.apply(annual_lambda)
df.to_pickle('/Users/xuegeng/Spider_Workspace/crawler/shop_datas.pkl')
df.to_excel('/Users/xuegeng/Spider_Workspace/crawler/shop_datas.xlsx')