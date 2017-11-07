# -*- coding: utf-8 -*-
import re,json,os,itertools
from pymongo import MongoClient
from CrawlFunctions import getSoup
from parseItems import parse_item
import numpy as np
import pandas as pd
from datetime import datetime
from collections import Counter
from multiprocessing.dummy import Lock,Pool

client = MongoClient()
clct = client.foodstore.bjfoodstore
mongo_lock = Lock()
format_json_filename = '/Users/xuegeng/Spider_Workspace/crawler/shop_comment_parsed/%s.json'
global total_df
def return_resolved_ids():
    from os import listdir
    from os.path import isfile, join
    onlyfiles = [f for f in listdir('/Users/xuegeng/Spider_Workspace/crawler/shop_comment_parsed') if isfile(join('/Users/xuegeng/Spider_Workspace/crawler/shop_comment_parsed', f))]
    onlyfiles.remove('.DS_Store')
    with open('/Users/xuegeng/Desktop/终端存储的输出.txt','r') as f:
        txt = f.read()  
    with open('/Users/xuegeng/Desktop/终端存储的输出2.txt','r') as f:
        txt2 = f.read()    
    with open('/Users/xuegeng/Desktop/终端存储的输出3.txt','r') as f:
        txt3 = f.read()
    with open('/Users/xuegeng/Desktop/终端存储的输出4.txt','r') as f:
        txt4 = f.read()
    with open('/Users/xuegeng/Desktop/终端存储的输出5.txt','r') as f:
        txt5 = f.read()
    with open('/Users/xuegeng/Desktop/终端存储的输出6.txt','r') as f:
        txt6 = f.read()    
    l = re.findall('\d{6,14}',txt+txt2+txt3+txt4+txt5+txt6)
    l2 = map(lambda x:x.split('.')[0],onlyfiles)
    l.extend(l2)
    return list(np.unique(l))
def return_all_id():
    fp = '/Users/xuegeng/Spider_Workspace/crawler/shop_urls.json'
    with open(fp,'r') as f:
        l = json.load(f)
    l2 = map(lambda x:x.split('/')[-1],l)
    return list(np.unique(l2))    
def return_unresolved_ids():
    all_ids = return_all_id()
    resolved_ids = return_resolved_ids()
    return list(set(all_ids)-set(resolved_ids))


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
    #孙航组的变量
    good_comm = df[df['total_rank']>=4]['serv']#好评数
    bad_comm = df[df['total_rank']<=2]['serv']#差评数
    comm = df['serv']#评论数
    good_comm_rpy = df[(df['total_rank']>=4)&df['has_reply']]['serv']#好评回复数
    bad_comm_rpy = df[(df['total_rank']<=2)&df['has_reply']]['serv']#差评回复数
    comm_rpy = df[df['has_reply']]['serv']#回复数
    comm_time = df[df['has_reply']]['comm_time']
    bus_rep_time = df[df['has_reply']]['bus_rep_time']
    reply_content_len = df[df['has_reply']]['reply_content_len']#回复长度
    
    try:good_comm_11 = good_comm['2016-11'].count()#十一月好评数
    except KeyError: good_comm_11  = 0
    try:bad_comm_11 = bad_comm['2016-11'].count()#十一月差评数
    except KeyError: bad_comm_11  = 0
    try:comm_11 = comm['2016-11'].count()#十一月评论数
    except KeyError: comm_11  = 0
    try:comm_rpy_11 = comm_rpy['2016-11'].count()#十一月回复数
    except KeyError:  comm_rpy_11 = 0
    try:good_comm_rpy_11 = good_comm_rpy['2016-11'].count()#十一月好评回复数
    except KeyError: good_comm_rpy_11  = 0
    try:bad_comm_rpy_11 =  bad_comm_rpy['2016-11'].count()#十一月差评回复数
    except KeyError: bad_comm_rpy_11  = 0
    try:comm_time_11 = comm_time['2016-11'].values.astype('datetime64[m]')#十一月评论时间
    except KeyError:  comm_time_11 = 0
    try: bus_rep_time_11 = bus_rep_time['2016-11'].values.astype('datetime64[m]')#十一月回复时间
    except KeyError: bus_rep_time_11  = 0
    try: delay_amount_11 = sum(bus_rep_time_11 - comm_time_11)/np.timedelta64(1, 'm')#十一月延迟总量
    except (KeyError,TypeError): delay_amount_11  = 0
    try:reply_content_len_amount_11 = reply_content_len['2016-11'].sum()#十一月回复总字数
    except KeyError: reply_content_len_amount_11  = 0
    
    try:good_comm_12 = good_comm['2016-12'].count()#十二月好评数
    except KeyError: good_comm_12  = 0
    try:bad_comm_12 = bad_comm['2016-12'].count()#十二月差评数
    except KeyError: bad_comm_12  = 0
    try:comm_12 = comm['2016-12'].count()#十二月评论数
    except KeyError: comm_12  = 0
    try:comm_rpy_12 = comm_rpy['2016-12'].count()#十二月回复数
    except KeyError: comm_rpy_12  = 0
    try:good_comm_rpy_12 = good_comm_rpy['2016-12'].count()#十二月好评回复数
    except KeyError: good_comm_rpy_12  = 0
    try:bad_comm_rpy_12 =  bad_comm_rpy['2016-12'].count()#十二月差评回复数
    except KeyError:  bad_comm_rpy_12 = 0
    try:comm_time_12 = comm_time['2016-12'].values.astype('datetime64[m]')#十二月评论时间
    except KeyError:  comm_time_12 = 0
    try:bus_rep_time_12 = bus_rep_time['2016-12'].values.astype('datetime64[m]')#十二月回复时间
    except KeyError: bus_rep_time_12  = 0
    try:delay_amount_12 = sum(bus_rep_time_12 - comm_time_12)/np.timedelta64(1, 'm')#十二月延迟总量
    except (KeyError,TypeError):  delay_amount_12 = 0
    try:reply_content_len_amount_12 = reply_content_len['2016-12'].sum()#十二月回复总字数
    except KeyError:  reply_content_len_amount_12 = 0
    
    try:good_comm_01 = good_comm['2017-01'].count()#一月好评数
    except KeyError: good_comm_01  = 0
    try:bad_comm_01 = bad_comm['2017-01'].count()#一月差评数
    except KeyError: bad_comm_01  = 0
    try:comm_01 = comm['2017-01'].count()#一月评论数
    except KeyError: comm_01  = 0
    try:comm_rpy_01 = comm_rpy['2017-01'].count()#一月回复数
    except KeyError: comm_rpy_01  = 0
    try:good_comm_rpy_01 = good_comm_rpy['2017-01'].count()#一月好评回复数
    except KeyError: good_comm_rpy_01  = 0
    try:bad_comm_rpy_01 =  bad_comm_rpy['2017-01'].count()#一月差评回复数
    except KeyError: bad_comm_rpy_01  = 0
    try:comm_time_01 = comm_time['2017-01'].values.astype('datetime64[m]')#一月评论时间
    except KeyError: comm_time_01  = 0
    try:bus_rep_time_01 = bus_rep_time['2017-01'].values.astype('datetime64[m]')#一月回复时间
    except KeyError: bus_rep_time_01  = 0
    try:delay_amount_01 = sum(bus_rep_time_01 - comm_time_01)/np.timedelta64(1, 'm')#一月延迟总量
    except (KeyError,TypeError):  delay_amount_01 = 0
    try:reply_content_len_amount_01 = reply_content_len['2017-01'].sum()#一月回复总字数
    except KeyError: reply_content_len_amount_01  = 0
    
    try:good_comm_02 = good_comm['2017-02'].count()#二月好评数
    except KeyError: good_comm_02  = 0
    try:bad_comm_02 = bad_comm['2017-02'].count()#二月差评数
    except KeyError: bad_comm_02  = 0
    try:comm_02 = comm['2017-02'].count()#二月评论数
    except KeyError: comm_02  = 0
    try:comm_rpy_02 = comm_rpy['2017-02'].count()#二月回复数
    except KeyError: comm_rpy_02  = 0
    try:good_comm_rpy_02 = good_comm_rpy['2017-02'].count()#二月好评回复数
    except KeyError:  good_comm_rpy_02 = 0
    try:bad_comm_rpy_02 =  bad_comm_rpy['2017-02'].count()#二月差评回复数
    except KeyError: bad_comm_rpy_02  = 0
    try:comm_time_02 = comm_time['2017-02'].values.astype('datetime64[m]')#二月评论时间
    except KeyError: comm_time_02  = 0
    try:bus_rep_time_02 = bus_rep_time['2017-02'].values.astype('datetime64[m]')#二月回复时间
    except KeyError: bus_rep_time_02  = 0
    try:delay_amount_02 = sum(bus_rep_time_02 - comm_time_02)/np.timedelta64(1, 'm')#二月延迟总量
    except (KeyError,TypeError): delay_amount_02  = 0
    try:reply_content_len_amount_02 = reply_content_len['2017-02'].sum()#二月回复总字数
    except KeyError:  reply_content_len_amount_02 = 0
    
    try:good_comm_03 = good_comm['2017-03'].count()#三月好评数
    except KeyError: good_comm_03  = 0
    try:bad_comm_03 = bad_comm['2017-03'].count()#三月差评数
    except KeyError: bad_comm_03  = 0
    try:comm_03 = comm['2017-03'].count()#三月评论数
    except KeyError: comm_03  = 0
    try:comm_rpy_03 = comm_rpy['2017-03'].count()#三月回复数
    except KeyError: comm_rpy_03  = 0
    try:good_comm_rpy_03 = good_comm_rpy['2017-03'].count()#三月好评回复数
    except KeyError:  good_comm_rpy_03 = 0
    try:bad_comm_rpy_03 =  bad_comm_rpy['2017-03'].count()#三月差评回复数
    except KeyError: bad_comm_rpy_03  = 0
    try:comm_time_03 = comm_time['2017-03'].values.astype('datetime64[m]')#三月评论时间
    except KeyError: comm_time_03  = 0
    try:bus_rep_time_03 = bus_rep_time['2017-03'].values.astype('datetime64[m]')#三月回复时间
    except KeyError: bus_rep_time_03  = 0
    try:delay_amount_03 = sum(bus_rep_time_03 - comm_time_03)/np.timedelta64(1, 'm')#三月延迟总量
    except (KeyError,TypeError): delay_amount_03  = 0
    try:reply_content_len_amount_03 = reply_content_len['2017-03'].sum()#三月回复总字数
    except KeyError:  reply_content_len_amount_03 = 0
    
    try:good_comm_04 = good_comm['2017-04'].count()#四月好评数
    except KeyError: good_comm_04  = 0
    try:bad_comm_04 = bad_comm['2017-04'].count()#四月差评数
    except KeyError: bad_comm_04  = 0
    try:comm_04 = comm['2017-04'].count()#四月评论数
    except KeyError: comm_04  = 0
    try:comm_rpy_04 = comm_rpy['2017-04'].count()#四月回复数
    except KeyError:  comm_rpy_04 = 0
    try:good_comm_rpy_04 = good_comm_rpy['2017-04'].count()#四月好评回复数
    except KeyError: good_comm_rpy_04  = 0
    try:bad_comm_rpy_04 =  bad_comm_rpy['2017-04'].count()#四月差评回复数
    except KeyError: bad_comm_rpy_04  = 0
    try:comm_time_04 = comm_time['2017-04'].values.astype('datetime64[m]')#四月评论时间
    except KeyError: comm_time_04  = 0
    try:bus_rep_time_04 = bus_rep_time['2017-04'].values.astype('datetime64[m]')#四月回复时间
    except KeyError: bus_rep_time_04  = 0
    try:delay_amount_04 = sum(bus_rep_time_04 - comm_time_04)/np.timedelta64(1, 'm')#四月延迟总量
    except (KeyError,TypeError): delay_amount_04  = 0
    try:reply_content_len_amount_04 = reply_content_len['2017-04'].sum()#四月回复总字数
    except KeyError:  reply_content_len_amount_04 = 0
    
   
    
    #薛耕组变量
    first_comment_time = min(df.index)#第一条评论时间
    store_age = (datetime.now()-first_comment_time).days#开店时间
    try:df2 = df['2016-04-01':'2016-05-20']#限定在此范围
    except KeyError: return
    try:first_comment_time2 = min(df2.index)#第一条评论时间
    except ValueError:first_comment_time2 = None
    has_pic_comm = df2[df2.has_picture].count()['serv']#带图评论数量
    total_comm = df2.count()['serv']#总评论数
    total_bus_rpl_comm = df2.has_reply.count()#商家回复数
    avg_heart_num = df2.heart_num.mean()#平均点赞数
    avg_recomm_num = df2.recomment_num.mean()#平均评论2数
    avg_bus_rpl_comm = total_bus_rpl_comm/max(1.0,float(total_comm))#商家回复占比
    sum_heart_num = df2.heart_num.sum()#点赞数
    sum_recomm_num = df2.recomment_num.sum()#评论2数
    heart_comm_rate = df2[df2.heart_num>0]['serv'].count()/max(1.0,float(total_comm))#点赞评论占比
    recomm_comm_rate = df2[df2.recomment_num>0]['serv'].count()/max(1.0,float(total_comm))#评论2占比
    c = Counter(list(df2.com_id))
    dd = pd.DataFrame(c.items(),columns =['k','v'])
    laters = dd[dd['v']>1].v.count()#回头客数量
    laters_freq = dd[dd['v']>1].v.sum()/max(1.0,float(laters))#回头客平均光顾次数
    avg_laters_csm = laters/max(1.0,float(dd.count()[0]))#回头客占比
    
    try:df2 = df['2016-01-31':'2016-03-31']#限定在此范围
    except KeyError: return
    try:first_comment_time3 = min(df2.index)#第一条评论时间
    except ValueError:first_comment_time3 = None
    has_pic_comm2 = df2[df2.has_picture].count()['serv']#带图评论数量
    total_comm2 = df2.count()['serv']#总评论数
    total_bus_rpl_comm2 = df2.has_reply.count()#商家回复数
    avg_heart_num2 = df2.heart_num.mean()#平均点赞数
    avg_recomm_num2 = df2.recomment_num.mean()#平均评论2数
    avg_bus_rpl_comm2 = total_bus_rpl_comm2/max(1.0,float(total_comm2))#商家回复占比
    sum_heart_num2 = df2.heart_num.sum()#点赞数
    sum_recomm_num2 = df2.recomment_num.sum()#评论2数
    heart_comm_rate2 = df2[df2.heart_num>0]['serv'].count()/max(1.0,float(total_comm2))#点赞评论占比
    recomm_comm_rate2 = df2[df2.recomment_num>0]['serv'].count()/max(1.0,float(total_comm2))#评论2占比
    c = Counter(list(df2.com_id))
    dd = pd.DataFrame(c.items(),columns =['k','v'])
    laters2 = dd[dd['v']>1].v.count()#回头客数量
    laters_freq2 = dd[dd['v']>1].v.sum()/max(1.0,float(laters2))#回头客平均光顾次数
    avg_laters_csm2 = laters/max(1.0,float(dd.count()[0]))#回头客占比
    
    
    shop_id = df['shop_id'][0]
    return dict(
                #孙航组的变量
                #good_comm = good_comm,#好评数，
                #bad_comm = bad_comm,#差评数
                #comm = comm,#评论数
                #good_comm_rpy = good_comm_rpy,#好评回复数
                #bad_comm_rpy = bad_comm_rpy,#差评回复数
                #comm_rpy = comm_rpy,#回复数
                #reply_content_len = reply_content_len,#回复长度
                
                good_comm_11 = good_comm_11,#十一月好评数
                bad_comm_11 = bad_comm_11,#十一月差评数
                comm_11 = comm_11,#十一月评论数
                comm_rpy_11 = comm_rpy_11,#十一月回复数
                good_comm_rpy_11 =  good_comm_rpy_11,#十一月好评回复数
                bad_comm_rpy_11 =  bad_comm_rpy_11,#十一月差评回复数
                delay_amount_11 = delay_amount_11,#十一月延迟总量
                reply_content_len_amount_11 = reply_content_len_amount_11,#十一月回复总字数
                
                good_comm_12 = good_comm_12,#十二月好评数
                bad_comm_12 = bad_comm_12,#十二月差评数
                comm_12 = comm_12,#十二月评论数
                comm_rpy_12 = comm_rpy_12,#十二月回复数
                good_comm_rpy_12 = good_comm_rpy_12,#十二月好评回复数
                bad_comm_rpy_12 =  bad_comm_rpy_12,#十二月差评回复数
                delay_amount_12 = delay_amount_12,#十二月延迟总量
                reply_content_len_amount_12 = reply_content_len_amount_12,#十二月回复总字数
                
                good_comm_01 = good_comm_01,#一月好评数
                bad_comm_01 = bad_comm_01,#一月差评数
                comm_01 = comm_01,#一月评论数
                comm_rpy_01 = comm_rpy_01,#一月回复数
                good_comm_rpy_01 = good_comm_rpy_01,#一月好评回复数
                bad_comm_rpy_01 =  bad_comm_rpy_01,#一月差评回复数
                delay_amount_01 = delay_amount_01,#一月延迟总量
                reply_content_len_amount_01 = reply_content_len_amount_01,#一月回复总字数
                
                good_comm_02 = good_comm_02,#二月好评数
                bad_comm_02 = bad_comm_02,#二月差评数
                comm_02 = comm_02,#二月评论数
                comm_rpy_02 = comm_rpy_02,#二月回复数
                good_comm_rpy_02 = good_comm_rpy_02,#二月好评回复数
                bad_comm_rpy_02 =  bad_comm_rpy_02,#二月差评回复数
                delay_amount_02 = delay_amount_02,#二月延迟总量
                reply_content_len_amount_02 = reply_content_len_amount_02,#二月回复总字数
                
                good_comm_03 = good_comm_03,#三月好评数
                bad_comm_03 = bad_comm_03,#三月差评数
                comm_03 = comm_03,#三月评论数
                comm_rpy_03 = comm_rpy_03,#三月回复数
                good_comm_rpy_03 = good_comm_rpy_03,#三月好评回复数
                bad_comm_rpy_03 =  bad_comm_rpy_03,#三月差评回复数
                delay_amount_03 = delay_amount_03,#三月延迟总量
                reply_content_len_amount_03 = reply_content_len_amount_03,#三月回复总字数
                
                good_comm_04 = good_comm_04,#四月好评数
                bad_comm_04 = bad_comm_04,#四月差评数
                comm_04 = comm_04,#四月评论数
                comm_rpy_04 = comm_rpy_04,#四月回复数
                good_comm_rpy_04 = good_comm_rpy_04,#四月好评回复数
                bad_comm_rpy_04 =  bad_comm_rpy_04,#四月差评回复数
                delay_amount_04 = delay_amount_04,#四月延迟总量
                reply_content_len_amount_04 = reply_content_len_amount_04,#四月回复总字数
                
             
                
                #薛耕组变量
                first_comment_time = str(first_comment_time),#第一条评论时间
                store_age = store_age,#开店时间
                first_comment_time2 = str(first_comment_time2),#第一条评论时间
                has_pic_comm = has_pic_comm,#带图评论数量
                total_comm = total_comm,#总评论数
                total_bus_rpl_comm = total_bus_rpl_comm,#商家回复数
                avg_heart_num = avg_heart_num,#平均点赞数
                avg_recomm_num = avg_recomm_num,#平均评论2数
                avg_bus_rpl_comm = avg_bus_rpl_comm,#商家回复占比
                sum_heart_num = sum_heart_num,#点赞数
                sum_recomm_num = sum_recomm_num,#评论2数
                heart_comm_rate = heart_comm_rate,#点赞评论占比
                recomm_comm_rate = recomm_comm_rate,#评论2占比
                laters = laters,#回头客数量
                laters_freq = laters_freq,#回头客平均光顾次数
                avg_laters_csm = avg_laters_csm,#回头客占比
                
             
                first_comment_time3 = str(first_comment_time3),#第一条评论时间
                has_pic_comm2 = has_pic_comm2,#带图评论数量
                total_comm2 = total_comm2,#总评论数
                total_bus_rpl_comm2 = total_bus_rpl_comm2,#商家回复数
                avg_heart_num2 = avg_heart_num2,#平均点赞数
                avg_recomm_num2 = avg_recomm_num2,#平均评论2数
                avg_bus_rpl_comm2 = avg_bus_rpl_comm2,#商家回复占比
                sum_heart_num2 = sum_heart_num2,#点赞数
                sum_recomm_num2 = sum_recomm_num2,#评论2数
                heart_comm_rate2 = heart_comm_rate2,#点赞评论占比
                recomm_comm_rate2 = recomm_comm_rate2,#评论2占比
                laters2 = laters2,#回头客数量
                laters_freq2 = laters_freq2,#回头客平均光顾次数
                avg_laters_csm2 = avg_laters_csm2,#回头客占比
                
                
                shop_id = shop_id
                )

def dict_saved_to_json(d):
    shop_id = d['shop_id']
    with open(format_json_filename%shop_id,'w') as f:
        json.dump(d,f)  
         



def id_to_df(id_):
    global clct,mongo_lock
    with mongo_lock:
        cur = clct.find({'shop_id':id_}
                                ,{'content':0,'reply_content':0})
    return pd.DataFrame(list(cur))
  
def ids_to_df(ids):
    global clct,mongo_lock
    with mongo_lock:
        cur = clct.find({'shop_id':
                                {'$in':ids}}
                                ,{'content':0,'reply_content':0})
    return pd.DataFrame(list(cur))   
   

def describe():
    try:
        ids_len = len(return_resolved_ids())
        print "[=] %d shops solved"%ids_len
        print "[=] %d shops to solved"%(99167-ids_len)
    except:
        pass
        
#ids = return_unresolved_ids()[40000:]
#total_df = ids_to_df(ids)
       
def worker_job(id_):
    global mongo_lock,data_append_lock,global_data_list,total_df
    print "[+] Processing %s"%id_
    df = total_df[total_df.shop_id  == id_]
    try:
        dict_ = gather_required_data(df)
        dict_saved_to_json(dict_)
    except ZeroDivisionError:
        print "[-] Math error at : " +id_
        return
    except AttributeError:
        print "[-] Empty shop : " +id_
        return
    except KeyError:
        print "[-] Didn't make it shop : " +id_
        return
    except ValueError:
        print "[-] Value error shop : " +id_
        return
    except IndexError:
        print "[-] Index Error at " + id_
        return
    
    describe()
        

def process_worker():
    global total_df,ids
    pool = Pool(processes = 30)
    results = pool.map(worker_job, ids)
    pool.close()
    pool.join()


#try:
#    process_worker()
#except KeyboardInterrupt:
#    print "HAHAHAHAHAHAHAHA"

#data = gather_required_data(id_to_df("68717373"))#用来检测

#in_fp = '/Users/xuegeng/Spider_Workspace/crawler/shop_datas.json'
#id_solved_fp = '/Users/xuegeng/Spider_Workspace/crawler/is_id_solved.json'
#with open(in_fp,'w') as f:
#            json.dump([],f)
#with open(id_solved_fp,'w') as f:
#            json.dump(dict(map(lambda x:(x,False),return_all_id())),f)