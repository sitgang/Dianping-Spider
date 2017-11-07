# -*- coding: utf-8 -*-
import pandas as pd
import datetime
import re

delta = datetime.timedelta(days=7)
down_limit = datetime.datetime(2015,10,1)
up_limit = datetime.datetime(2016,9,29)

d = down_limit
dlist=[]
while d <= up_limit:
    dlist.append(d)
    d+=delta
xg_dlist=map(lambda x:x.strftime('%Y-%m-%d'),dlist)

df = pd.read_excel('/Users/xuegeng/Desktop/电子商务编程/tofyw3.xlsx')
#df = pd.read_excel('/Users/xuegeng/Desktop/电子商务编程/testdf.xlsx')
del df['env']
del df['serv']
del df['taste']
del df['total_rank']
df.index = range(len(df))
df = df[(df['serv2015-10-01']+df['taste2015-10-01']+df['env2015-10-01'])/3>=4.0]
df.index = range(len(df))


re.findall('\d+-\d+-\d+','taste2016-08-18')[0]
re.findall('[a-z]+_?[a-z]+_?[a-z]+_?[a-z]+','has_pic_comm2015-10-29')[0]

date_num_map=[]
for e,v in enumerate(xg_dlist):
    date_num_map.append((v,e+1))

date_num_map = dict(date_num_map)

def deal_with_var(var):
    
    if 'env' in var:
        prefix = [u'env']
        var_date = re.findall('\d+-\d+-\d+',var)
    else:
        prefix = re.findall('[a-z]+_?[a-z]+_?[a-z]+_?[a-z]+',var)
        var_date = re.findall('\d+-\d+-\d+',var)

    if not var_date:#那么就是控制变量
        kind = 0
        return (kind,prefix[0])
    else:
        kind = 1
        return (kind,prefix[0],var_date[0])

def change_dict(kind_fix,value,main_dict):
    
    if kind_fix[0] == 0:
        
        main_dict[kind_fix[1]]=dict(zip(range(1,54),[value]*53))

    else:
        try:
            main_dict[kind_fix[1]][date_num_map[kind_fix[2]]] = value
        except KeyError:
            main_dict[kind_fix[1]] = {}
            main_dict[kind_fix[1]][date_num_map[kind_fix[2]]] = value
            
    return main_dict
    
def deal_with_kv(kv,main_dict):
    
    k,v = kv
    kind_fix = deal_with_var(k)
    main_dict = change_dict(kind_fix,v,main_dict)
    return main_dict

num_dict = {'num':{}}
for i in range(1,54):
    num_dict['num'][i]=i

def id_to_dict(id_):
    global df,ids,num_dict
    
    sr = df[df.shop_id==id_]
    single_list = sr.T.to_dict().values()[0].items()
    main_dict = {}
    for item in single_list:
        main_dict = deal_with_kv(item,main_dict)
    main_dict['num'] =  num_dict['num']
    return main_dict




di={'serv':{0:41,1:523},'taste':{0:123,1:1234}}
di2={'serv':{0:41,1:523},'taste':{0:123,1:1234}}



ids = list(df.shop_id)
count=0


#dfs = []
#for id_ in ids:
#    try:
#        dfs.append(pd.DataFrame(id_to_dict(id_)))
#    except:
#        count+=1
#        print count
#
#dftotal = pd.concat(dfs)
#
#dftotal.to_excel('/Users/xuegeng/Desktop/电子商务编程/tofyw5gte4.xlsx')






















