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
from collections import Counter
from multiprocessing.dummy import Lock,Pool

client = MongoClient()
clct = client.foodstore.bjfoodstore
mongo_lock = Lock()
format_json_filename = '/Users/xuegeng/Spider_Workspace/crawler/shop_comment_parsed/%s.json'


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






def gather_required_data(df):
    
    df = clean_df2(df)
    df['has_reply'] = df.reply_content_len>0#增加一列来辅助，表示该评论有回复
   
    
    shop_id = df['shop_id'][0]
    return dict(
                #孙航组的变量
                good_comm = df[df['total_rank']>=4]['serv'],#好评数
                bad_comm = df[df['total_rank']<=2]['serv'],#差评数
                comm = df['serv'],#评论数
                good_comm_rpy = df[(df['total_rank']>=4)&df['has_reply']]['serv'],#好评回复数
                bad_comm_rpy = df[(df['total_rank']<=2)&df['has_reply']]['serv'],#差评回复数
                comm_rpy = df[df['has_reply']]['serv'],#回复数
                comm_time = df[df['has_reply']]['comm_time'],
                bus_rep_time = df[df['has_reply']]['bus_rep_time'],
                reply_content_len = df[df['has_reply']]['reply_content_len']#回复长度
                
                good_comm_11 = good_comm['2016-11'].count()#十一月好评数
                bad_comm_11 = bad_comm['2016-11'].count()#十一月差评数
                comm_11 = comm['2016-11'].count()#十一月评论数
                comm_rpy_11 = comm_rpy['2016-11'].count()#十一月回复数
                good_comm_rate_11 = float(good_comm_11)/max(1,comm_11)#十一月好评率
                bad_comm_rate_11 = float(bad_comm_11)/max(1,comm_11)#十一月坏评率
                good_comm_rpy_11 = good_comm_rpy['2016-11'].count()#十一月好评回复数
                bad_comm_rpy_11 =  bad_comm_rpy['2016-11'].count()#十一月差评回复数
                good_comm_rpy_rate_11 = float(good_comm_rpy_11)/max(1,good_comm_11)#十一月好评回复率
                bad_comm_rpy_rate_11 = float(bad_comm_rpy_11)/max(1,bad_comm_11)#十一月坏评回复率
                comm_time_11 = comm_time['2016-11'].values.astype('datetime64[m]')#十一月评论时间
                bus_rep_time_11 = bus_rep_time['2016-11'].values.astype('datetime64[m]')#十一月回复时间
                delay_amount_11 = sum(bus_rep_time_11 - comm_time_11)/np.timedelta64(1, 'm')#十一月延迟总量
                avg_delay_11 = delay_amount_11/comm_rpy_11#十一月平均回复延迟
                reply_content_len_amount_11 = reply_content_len['2016-11'].sum()#十一月回复总字数
                avg_len_11 = float(reply_content_len_amount_11)/max(1,comm_rpy_11)#十一月平均回复字数
                
                good_comm_12 = good_comm['2016-12'].count()#十二月好评数
                bad_comm_12 = bad_comm['2016-12'].count()#十二月差评数
                comm_12 = comm['2016-12'].count()#十二月评论数
                comm_rpy_12 = comm_rpy['2016-12'].count()#十二月回复数
                good_comm_rate_12 = float(good_comm_12)/max(1,comm_12)#十二月好评率
                bad_comm_rate_12 = float(bad_comm_12)/max(1,comm_12)#十二月坏评率
                good_comm_rpy_12 = good_comm_rpy['2016-12'].count()#十二月好评回复数
                bad_comm_rpy_12 =  bad_comm_rpy['2016-12'].count()#十二月差评回复数
                good_comm_rpy_rate_12 = float(good_comm_rpy_12)/max(1,good_comm_12)#十二月好评回复率
                bad_comm_rpy_rate_12 = float(bad_comm_rpy_12)/max(1,bad_comm_12)#十二月坏评回复率
                comm_time_12 = comm_time['2016-12'].values.astype('datetime64[m]')#十二月评论时间
                bus_rep_time_12 = bus_rep_time['2016-12'].values.astype('datetime64[m]')#十二月回复时间
                delay_amount_12 = sum(bus_rep_time_12 - comm_time_12)/np.timedelta64(1, 'm')#十二月延迟总量
                avg_delay_12 = delay_amount_12/comm_rpy_12#十二月平均回复延迟
                reply_content_len_amount_12 = reply_content_len['2016-12'].sum()#十二月回复总字数
                avg_len_12 = float(reply_content_len_amount_12)/max(1,comm_rpy_12)#十二月平均回复字数
                
                good_comm_01 = good_comm['2017-01'].count()#一月好评数
                bad_comm_01 = bad_comm['2017-01'].count()#一月差评数
                comm_01 = comm['2017-01'].count()#一月评论数
                comm_rpy_01 = comm_rpy['2017-01'].count()#一月回复数
                good_comm_rate_01 = float(good_comm_01)/max(1,comm_01)#一月好评率
                bad_comm_rate_01 = float(bad_comm_01)/max(1,comm_01)#一月坏评率
                good_comm_rpy_01 = good_comm_rpy['2017-01'].count()#一月好评回复数
                bad_comm_rpy_01 =  bad_comm_rpy['2017-01'].count()#一月差评回复数
                good_comm_rpy_rate_01 = float(good_comm_rpy_01)/max(1,good_comm_01)#一月好评回复率
                bad_comm_rpy_rate_01 = float(bad_comm_rpy_01)/max(1,bad_comm_01)#一月坏评回复率
                comm_time_01 = comm_time['2017-01'].values.astype('datetime64[m]')#一月评论时间
                bus_rep_time_01 = bus_rep_time['2017-01'].values.astype('datetime64[m]')#一月回复时间
                delay_amount_01 = sum(bus_rep_time_01 - comm_time_01)/np.timedelta64(1, 'm')#一月延迟总量
                avg_delay_01 = delay_amount_01/comm_rpy_01#一月平均回复延迟
                reply_content_len_amount_01 = reply_content_len['2017-01'].sum()#一月回复总字数
                avg_len_01 = float(reply_content_len_amount_01)/max(1,comm_rpy_01)#一月平均回复字数
                
                good_comm_02 = good_comm['2017-02'].count()#二月好评数
                bad_comm_02 = bad_comm['2017-02'].count()#二月差评数
                comm_02 = comm['2017-02'].count()#二月评论数
                comm_rpy_02 = comm_rpy['2017-02'].count()#二月回复数
                good_comm_rate_02 = float(good_comm_02)/max(1,comm_02)#二月好评率
                bad_comm_rate_02 = float(bad_comm_02)/max(1,comm_02)#二月坏评率
                good_comm_rpy_02 = good_comm_rpy['2017-02'].count()#二月好评回复数
                bad_comm_rpy_02 =  bad_comm_rpy['2017-02'].count()#二月差评回复数
                good_comm_rpy_rate_02 = float(good_comm_rpy_02)/max(1,good_comm_02)#二月好评回复率
                bad_comm_rpy_rate_02 = float(bad_comm_rpy_02)/max(1,bad_comm_02)#二月坏评回复率
                comm_time_02 = comm_time['2017-02'].values.astype('datetime64[m]')#二月评论时间
                bus_rep_time_02 = bus_rep_time['2017-02'].values.astype('datetime64[m]')#二月回复时间
                delay_amount_02 = sum(bus_rep_time_02 - comm_time_02)/np.timedelta64(1, 'm')#二月延迟总量
                avg_delay_02 = delay_amount_02/comm_rpy_02#二月平均回复延迟
                reply_content_len_amount_02 = reply_content_len['2017-02'].sum()#二月回复总字数
                avg_len_02 = float(reply_content_len_amount_02)/max(1,comm_rpy_02)#二月平均回复字数
                
                good_comm_03 = good_comm['2017-03'].count()#三月好评数
                bad_comm_03 = bad_comm['2017-03'].count()#三月差评数
                comm_03 = comm['2017-03'].count()#三月评论数
                comm_rpy_03 = comm_rpy['2017-03'].count()#三月回复数
                good_comm_rate_03 = float(good_comm_03)/max(1,comm_03)#三月好评率
                bad_comm_rate_03 = float(bad_comm_03)/max(1,comm_03)#三月坏评率
                good_comm_rpy_03 = good_comm_rpy['2017-03'].count()#三月好评回复数
                bad_comm_rpy_03 =  bad_comm_rpy['2017-03'].count()#三月差评回复数
                good_comm_rpy_rate_03 = float(good_comm_rpy_03)/max(1,good_comm_03)#三月好评回复率
                bad_comm_rpy_rate_03 = float(bad_comm_rpy_03)/max(1,bad_comm_03)#三月坏评回复率
                comm_time_03 = comm_time['2017-03'].values.astype('datetime64[m]')#三月评论时间
                bus_rep_time_03 = bus_rep_time['2017-03'].values.astype('datetime64[m]')#三月回复时间
                delay_amount_03 = sum(bus_rep_time_03 - comm_time_03)/np.timedelta64(1, 'm')#三月延迟总量
                avg_delay_03 = delay_amount_03/comm_rpy_03#三月平均回复延迟
                reply_content_len_amount_03 = reply_content_len['2017-03'].sum()#三月回复总字数
                avg_len_03 = float(reply_content_len_amount_03)/max(1,comm_rpy_03)#三月平均回复字数
                
                good_comm_04 = good_comm['2017-04'].count()#四月好评数
                bad_comm_04 = bad_comm['2017-04'].count()#四月差评数
                comm_04 = comm['2017-04'].count()#四月评论数
                comm_rpy_04 = comm_rpy['2017-04'].count()#四月回复数
                good_comm_rate_04 = float(good_comm_04)/max(1,comm_04)#四月好评率
                bad_comm_rate_04 = float(bad_comm_04)/max(1,comm_04)#四月坏评率
                good_comm_rpy_04 = good_comm_rpy['2017-04'].count()#四月好评回复数
                bad_comm_rpy_04 =  bad_comm_rpy['2017-04'].count()#四月差评回复数
                good_comm_rpy_rate_04 = float(good_comm_rpy_04)/max(1,good_comm_04)#四月好评回复率
                bad_comm_rpy_rate_04 = float(bad_comm_rpy_04)/max(1,bad_comm_04)#四月坏评回复率
                comm_time_04 = comm_time['2017-04'].values.astype('datetime64[m]')#四月评论时间
                bus_rep_time_04 = bus_rep_time['2017-04'].values.astype('datetime64[m]')#四月回复时间
                delay_amount_04 = sum(bus_rep_time_04 - comm_time_04)/np.timedelta64(1, 'm')#四月延迟总量
                avg_delay_04 = delay_amount_04/comm_rpy_04#四月平均回复延迟
                reply_content_len_amount_04 = reply_content_len['2017-04'].sum()#四月回复总字数
                avg_len_04 = float(reply_content_len_amount_04)/max(1,comm_rpy_04)#四月平均回复字数
                
                good_comm_six = sum([good_comm_11,good_comm_12,good_comm_01,good_comm_02,good_comm_03,good_comm_04])#六个月好评数
                bad_comm_six = sum([bad_comm_11,bad_comm_12,bad_comm_01,bad_comm_02,bad_comm_03,bad_comm_04])#六个月差评数
                comm_six = sum([comm_11,comm_12,comm_01,comm_02,comm_03,comm_04])#六个月评论数
                comm_rpy_six = sum([comm_rpy_11,comm_rpy_12,comm_rpy_01,comm_rpy_02,comm_rpy_03,comm_rpy_04])#六个月回复数
                good_comm_rate_six = float(good_comm_six)/max(1,comm_six)#六个月好评率
                bad_comm_rate_six = float(bad_comm_six)/max(1,comm_six)#六个月坏评率
                good_comm_rpy_six = sum([good_comm_rpy_11,good_comm_rpy_12,good_comm_rpy_01,good_comm_rpy_02,good_comm_rpy_03,good_comm_rpy_04])#六个月好评回复数
                bad_comm_rpy_six =  sum([bad_comm_rpy_11,bad_comm_rpy_12,bad_comm_rpy_01,bad_comm_rpy_02,bad_comm_rpy_03,bad_comm_rpy_04])#六个月差评回复数
                good_comm_rpy_rate_six = float(good_comm_rpy_six)/max(1,good_comm_six)#六个月好评回复率
                bad_comm_rpy_rate_six = float(bad_comm_rpy_six)/max(1,bad_comm_six)#六个月坏评回复率
                delay_amount_six = sum(delay_amount_11,delay_amount_12,delay_amount_01,delay_amount_02,delay_amount_03,delay_amount_04)#六个月延迟总量
                avg_delay_six = delay_amount_six/comm_rpy_six#六个月平均回复延迟
                reply_content_len_amount_six = sum(reply_content_len_amount_11,reply_content_len_amount_12,reply_content_len_amount_01,reply_content_len_amount_02,reply_content_len_amount_03,reply_content_len_amount_04)#六个月回复总字数
                avg_len_six = float(reply_content_len_amount_six)/max(1,comm_rpy_six)#六个月平均回复字数
                
                
                #薛耕组变量
                first_comment_time = min(df.index)#第一条评论时间
                store_age = (datetime.now()-first_comment_time).days#开店时间
                df2 = df['2017-01-28':'2017-04-28']#限定在此范围
                first_comment_time2 = min(df2.index)#第一条评论时间
                has_pic_comm = df2[df2.has_picture].count()['serv']#带图评论数量
                total_comm = df2.count()['serv']#总评论数
                total_bus_rpl_comm = df2.has_reply.count()#商家回复数
                avg_heart_num = df2.heart_num.mean()#平均点赞数
                avg_recomm_num = df2.recomment_num.mean()#平均评论2数
                avg_bus_rpl_comm = total_bus_rpl_comm/float(total_comm)#商家回复占比
                sum_heart_num = df2.heart_num.sum()#点赞数
                sum_recomm_num = df2.recomment_num.sum()#评论2数
                heart_comm_rate = df2[df2.heart_num>0]['serv'].count()/float(total_comm)#点赞评论占比
                recomm_comm_rate = df2[df2.recomment_num>0]['serv'].count()/float(total_comm)#评论2占比
                c = Counter(list(df2.com_id))
                dd = pd.DataFrame(c.items(),columns =['k','v'])
                laters = dd[dd['v']>1].v.count()#回头客数量
                laters_freq = dd[dd['v']>1].v.sum()/float(laters)#回头客平均光顾次数
                avg_laters_csm = laters/float(dd.count()[0])#回头客占比
                
                three_m_before_comm = df['2016-10-27':'2017-01-27'].count()[0]#之前三个月的总评论数
                groth_rate = total_comm/float(three_m_before_comm)#三个月相比的增长率
                
                shop_id = shop_id
                )

def dict_saved_to_json(d):
    shop_id = d['shop_id']
    with open(format_json_filename%shop_id,'w') as f:
        json.dump(d,f)  
         
def return_all_id():
    fp = '/Users/xuegeng/Spider_Workspace/crawler/shop_urls.json'
    with open(fp,'r') as f:
        l = json.load(f)
    l2 = map(lambda x:x.split('/')[-1],l)
    return list(np.unique(l2))


def id_to_df(id_):
    global clct,mongo_lock
    with mongo_lock:
        cur = clct.find({'shop_id':id_})
    return pd.DataFrame(list(cur))
    
def return_resolved_ids():
    from os import listdir
    from os.path import isfile, join
    onlyfiles = [f for f in listdir('/Users/xuegeng/Spider_Workspace/crawler/shop_comment_parsed') if isfile(join('/Users/xuegeng/Spider_Workspace/crawler/shop_comment_parsed', f))]
    onlyfiles.remove('.DS_Store')
    return map(lambda x:x.split('.')[0],onlyfiles)
    
def return_unresolved_ids():
    all_ids = return_all_id()
    resolved_ids = return_resolved_ids()
    return list(set(all_ids)-set(resolved_ids))
    
    
def describe():
    try:
        ids_len = len(return_resolved_ids())
        print "[=] %d shops solved"%ids_len
        print "[=] %d shops to solved"%(99167-ids_len)
    except:
        pass
        
        
def worker_job(id_):
    global mongo_lock,data_append_lock,global_data_list
    df = id_to_df(id_)
    try:
        dict_ = gather_required_data(df)
        dict_saved_to_json(dict_)
    except ZeroDivisionError:
        print "[-] Math error at : " +id_
    except AttributeError:
        print "[-] Empty shop : " +id_
    except IndexError:
        print "[-] Index Error at " + id_
        return
    if datetime.now().microsecond%2 == 0:
        describe()
        

def process_worker():
    ids = return_unresolved_ids()
    pool = Pool(processes = 30)
    results = pool.map(worker_job, ids)
    pool.close()
    pool.join()


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