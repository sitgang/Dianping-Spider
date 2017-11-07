# -*- coding: utf-8 -*-
import requests,re,json,pickle,os,itertools
from lxml import etree
from pymongo import MongoClient
from bs4 import BeautifulSoup
from CrawlFunctions import getSoup
from parseItems import parse_item
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from multiprocessing.dummy import Lock,Pool


with open('/Users/xuegeng/Spider_Workspace/crawler/is_id_solved.json','r') as f:
    is_id_solved = json.load(f)
client = MongoClient()
clct = client.foodstore.bjfoodstore
global_data_list = []
mongo_lock = Lock()
data_append_lock = Lock()


def parse_file(file_name):
    with open(file_name,'r') as f:
        text = f.read()
    soup = getSoup(text)
    shop_id = file_name.split('_')[-2]
    try:
        items = soup.find(class_="comment-list").find_all(id = re.compile("rev_"))
    except AttributeError:
        yield
    
    for item in items:
        try:
            d = parse_item(item)
            d['shop_id'] = shop_id
            yield d
        except:
            pass

def id_to_path(id_):
    dir_path = os.path.split(__file__)[0]+os.sep+'htmls'+os.sep+'shop_'+id_
    return map(lambda x:dir_path+os.sep+x,os.listdir(dir_path))
    

def filepath_to_diclist(file_path):
    return [x for x in parse_file(file_path)]

def id_to_diclist(id_):
    ls = map(filepath_to_diclist,id_to_path(id_))
    return [i for i in itertools.chain.from_iterable(ls)]

id_ = '17999157'









#x = id_to_diclist(id_)
#KeyboardInterrupt
#print len(x)

import statsmodels.api as sm
def fit_line2(x, y):
    """Return slope, intercept of best fit line."""
    X = sm.add_constant(x)
    model = sm.OLS(y, X, missing='drop') # ignores entires where x or y is NaN
    fit = model.fit()
    return fit.params[1], fit.params[0] # could also return stderr in each via fit.bse





#ucpi={2016:0.02,2015:1.0149,2014:1.0206,2013:1.0260,2012:1.0268,2011:1.0525,2010:1.0320,2009:0.9915,2008:1.0558,2007:1.0448,2006:1.0148}
def clean_df(dict_list):
    """return : pandas.core.frame.DataFrame"""
    df = pd.DataFrame(dict_list)
    df.index = pd.DatetimeIndex(df.comm_time)
    df = df.sort_index()
    df = df[~(np.abs(df.com_per-df.com_per.mean())>(3*df.com_per.std()))]#清洗出三个标准差之外的数据,人均有关的计算用df2
    return df

def clean_df2(df):
    """return : pandas.core.frame.DataFrame"""
    df.index = pd.DatetimeIndex(df.comm_time)
    df = df.sort_index()
    df = df[~(np.abs(df.com_per-df.com_per.mean())>(3*df.com_per.std()))]#清洗出三个标准差之外的数据,人均有关的计算用df2
    return df

def return_ave_com_per(df):
    """return : float"""
    #平均人均
    ave_com_per = df.mean()['com_per']#平均人均
    return ave_com_per

#means = df.resample('A',on='comm_time').mean()

#d1 = means['com_per']
#annual_com_per = zip(d1.index.year,d1.values)#年平均人均

def return_annual_resampler(df):
    '''return : pandas.tseries.resample.DatetimeIndexResampler'''
    return df.resample('A',on='comm_time')
def return_monthly_resampler(df):
    '''return : pandas.tseries.resample.DatetimeIndexResampler'''
    return df.resample('M',on='comm_time')
    
def return_annual_com_per_growth(annual_resampler):
    """    [(year int,com_per_growth float),()...]
            eg.
            [(2015, 1.0199212985735366),
            (2016, 0.94487112504353887),
            (2017, 1.2088951146909472)]"""
    #年平均人均增长率
    s1 = annual_resampler.mean()['com_per']
    price_groth = (s1/s1.shift(1)).dropna()#得到每年价格涨幅
    annual_com_per_growth = zip(price_groth.index.strftime('%Y-%m-%d'),price_groth.values)#年平均人均增长率
    return annual_com_per_growth

#m, b = fit_line2(df['env'], df['total_rank'])#得到环境对总评分的回归曲线

    
def lambda_for_comm_dense(x):
    try:
        return len(x)/float((x.index[-1]-x.index[0]).days)
    except (ZeroDivisionError,IndexError):
        return 0.0
def lambda_for_reply_rate(x):
    try:
        return len(x['bus_rep_time'].dropna())/float(len(x))
    except (ZeroDivisionError,IndexError):
        return 0.0
def lambda_for_five_star_ratio(x):
    try:
        return len(x[x['total_rank']==5])/float(len(x))
    except (ZeroDivisionError,IndexError):
        return 0.0

def return_annual_five_star_ratio(annual_resampler):
    """return : pandas.core.series.Series"""
    #每年的五星好评率
    annual_five_star_ratio = annual_resampler.agg(lambda_for_five_star_ratio)['total_rank']#每年的五星好评率
    return annual_five_star_ratio
    
def return_monthly_comm_dense(monthly_resampler):
    """[(year int ,month int,monthly_comm_dense float)...]
        eg.
        [(2014, 12, 0.49795918367346936),
        (2015, 12, 2.0576923076923075),
        (2016, 12, 1.2305555555555556),
        (2017, 12, 0.59829059829059827)]
    """
    #年度评论密度，评论数量除以天数,可以说是热度
    monthly_comm_dense = monthly_resampler.agg(lambda_for_comm_dense)#评论密度，评论数量除以天数,可以说是热度
    cd = monthly_comm_dense.ix[:,0]
    year_month_pop = zip(cd.index.strftime('%Y-%m-%d'),cd.values)#每月热度
    return year_month_pop

def return_comm_dense_before_after(df,dt = '2015-10-06'):
    """
        return : (comm_dense_before float,comm_dense_after float)
        eg.
        (1.2971428571428572, 1.2438596491228071)
    """
    #合并之前的评论密度
    comm_dense_before = df[:dt].apply(lambda_for_comm_dense)[0]#合并之前的评论密度
    comm_dense_after = df[dt:].apply(lambda_for_comm_dense)[0]#合并之后的评论密度
    return comm_dense_before,comm_dense_after

def return_annual_reply_rate(annual_resampler):
    """return : pandas.core.series.Series"""
    #商家每年的回复率
    annual_reply_rate = annual_resampler.agg(lambda_for_reply_rate)['bus_rep_time']#商家每年的回复率
    return annual_reply_rate
def return_reply_rate(df):
    """return : float"""
    #商家的回复率
    reply_rate = len(df['bus_rep_time'].dropna())/float(len(df))#商家的回复率
    return reply_rate
def return_five_star_ratio(df):
    """return : float"""
    #五星好评率
    five_star_ratio=len(df[df['total_rank']==5])/float(len(df))#五星好评率
    return five_star_ratio
def return_serv_taste_env_total_rank(df):
    """return : float,float,float,float"""
    #各均分
    serv,taste,env,total_rank = df.mean()[['serv','taste','env','total_rank']]#各均分
    return serv,taste,env,total_rank
def return_comm_dense(df):
    """return : float"""
    #顾客的评论密度，就是商家热度
    try:
        comm_dense = len(df)/float(np.abs((df.index[-1]-df.index[0]).days))#顾客的评论密度，就是商家热度
        return comm_dense
    except ZeroDivisionError:
        return 0.0
def return_five_star_ratio_groth(annual_five_star_ratio):
    """return : float"""
    #好评率的回归曲线斜率
    five_star_ratio_groth = fit_line2(annual_five_star_ratio.index.year, annual_five_star_ratio.values)[0]#好评率的回归曲线斜率
    return five_star_ratio_groth






def gather_required_data(df):
    
    df = clean_df2(df)
    annual_resampler = return_annual_resampler(df)
    monthly_resampler = return_monthly_resampler(df)
    monthly_comm_dense = return_monthly_comm_dense(monthly_resampler)#1
    ave_com_per = return_ave_com_per(df)#2
    annual_com_per_growth = return_annual_com_per_growth(annual_resampler)#3
    reply_rate = return_reply_rate(df)#4
    five_star_ratio = return_five_star_ratio(df)
    serv,taste,env,total_rank = return_serv_taste_env_total_rank(df)
    comm_dense = return_comm_dense(df)
    five_star_ratio_groth = return_five_star_ratio_groth(return_annual_five_star_ratio(annual_resampler))
    comm_dense_before_after = return_comm_dense_before_after(df)
    shop_id = df['shop_id'][0]
    return dict(
                monthly_comm_dense = monthly_comm_dense,
                ave_com_per = ave_com_per,
                annual_com_per_growth = annual_com_per_growth,
                reply_rate = reply_rate,
                five_star_ratio = five_star_ratio,
                serv = serv,
                taste = taste,
                env = env,
                total_rank = total_rank,
                comm_dense = comm_dense,
                five_star_ratio_groth = five_star_ratio_groth,
                comm_dense_before_after = comm_dense_before_after,
                shop_id = shop_id
                )
    
def return_all_id():
    fp = '/Users/xuegeng/Spider_Workspace/crawler/shop_urls.json'
    with open(fp,'r') as f:
        l = json.load(f)
    l2 = map(lambda x:x.split('/')[-1],l)
    return l2

def save_to_jsonfile(data_list):
    global is_id_solved
    in_fp = '/Users/xuegeng/Spider_Workspace/crawler/shop_datas.json'
    id_solved_fp = '/Users/xuegeng/Spider_Workspace/crawler/is_id_solved.json'
    #with open(in_fp,'r') as f:
    #    l = json.load(f)
    l = data_list
    with open(in_fp,'w') as f:
            json.dump(l,f)
    with open(id_solved_fp,'w') as f:
            json.dump(is_id_solved,f)

def mark_parsed(id_):
    global is_id_solved
    is_id_solved[id_] = True


def id_to_df(id_):
    global clct,mongo_lock
    with mongo_lock:
        cur = clct.find({'shop_id':id_})
    return pd.DataFrame(list(cur))
def describe():
    global is_id_solved
    try:
        count = 0
        for i in is_id_solved.iterkeys():
            if is_id_solved[i]:
                count += 1
        print "[=] %d shops solved"%count
        print "[=] %d shops to solved"%(len(is_id_solved)-count)
    except:
        pass
def worker_job(id_):
    global mongo_lock,data_append_lock,global_data_list
    df = id_to_df(id_)
    try:
        data = gather_required_data(df)
        with data_append_lock:
            global_data_list.append(data)
    except ZeroDivisionError:
        print "[-] Math error at : " +id_
    except AttributeError:
        print "[-] Empty shop : " +id_
    except IndexError:
        print "[-] Index Error at " + id_
        return
    mark_parsed(id_)
    if datetime.now().microsecond%2 == 0:
        describe()
        #save_to_jsonfile(global_data_list)
        
def get_unsolved_id():
    ##返回没有被解决的id
    global is_id_solved
    ids = []
    for i in is_id_solved.iterkeys():
            if not is_id_solved[i]:
                ids.append(i)
    return ids

def process_worker():
    global mongo_lock,data_append_lock,global_data_list,is_id_solved
    ids = get_unsolved_id()
    pool = Pool(processes = 100)
    results = pool.map(worker_job, ids)
    pool.close()
    pool.join()
    save_to_jsonfile(global_data_list)


#try:
#    process_worker()
#except KeyboardInterrupt:
#    save_to_jsonfile(global_data_list)

#data = gather_required_data(id_to_df("68717373"))#用来检测

#in_fp = '/Users/xuegeng/Spider_Workspace/crawler/shop_datas.json'
#id_solved_fp = '/Users/xuegeng/Spider_Workspace/crawler/is_id_solved.json'
#with open(in_fp,'w') as f:
#            json.dump([],f)
#with open(id_solved_fp,'w') as f:
#            json.dump(dict(map(lambda x:(x,False),return_all_id())),f)