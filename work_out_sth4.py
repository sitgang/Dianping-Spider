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
    
    three_m_before_comm = df['2016-10-27':'2017-01-27'].count()[0]#之前三个月的总评论数
    groth_rate = total_comm/max(1.0,float(three_m_before_comm))#三个月相比的增长率
    
    
    shop_id = df['shop_id'][0]
    return dict(
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
                
                three_m_before_comm = three_m_before_comm,#之前三个月的总评论数
                groth_rate = groth_rate,#三个月相比的增长率
                
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
    print "[+] Processing %s"%id_
    df = id_to_df(id_)
    try:
        dict_ = gather_required_data(df)
        dict_saved_to_json(dict_)
    except ValueError:
        print "[-] Someting is empty!"
        return
    except AttributeError:
        print "[-] Empty shop : " +id_
        return
    except KeyError:
        print "[-] Didn't make it shop : " +id_
        return
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


try:
    process_worker()
except KeyboardInterrupt:
    print "HAHAHAHAHAHAHAHA"

#data = gather_required_data(id_to_df("68717373"))#用来检测

#in_fp = '/Users/xuegeng/Spider_Workspace/crawler/shop_datas.json'
#id_solved_fp = '/Users/xuegeng/Spider_Workspace/crawler/is_id_solved.json'
#with open(in_fp,'w') as f:
#            json.dump([],f)
#with open(id_solved_fp,'w') as f:
#            json.dump(dict(map(lambda x:(x,False),return_all_id())),f)