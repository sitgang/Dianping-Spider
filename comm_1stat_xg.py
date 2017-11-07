# -*- coding: utf-8 -*-
import re,json,os,itertools
from pymongo import MongoClient
from CrawlFunctions import getSoup
from parseItems import parse_item
import numpy as np
import pandas as pd
import datetime
from collections import Counter
from multiprocessing.dummy import Lock,Pool


delta = datetime.timedelta(days=7)
down_limit = datetime.datetime(2015,10,1)
up_limit = datetime.datetime(2016,9,29)

d = down_limit
dlist=[]
while d <= up_limit:
    dlist.append(d)
    d+=delta
dlist=map(lambda x:x.strftime('%Y-%m-%d'),dlist)

client = MongoClient()
clct = client.foodstore.bjfoodstore
mongo_lock = Lock()
format_json_filename = '/Users/xuegeng/Spider_Workspace/crawler/shop_comment_parsed_xg2/%s.json'
global total_df
all_shopid_json_file = '/Users/xuegeng/Desktop/电子商务编程/628lunwen/shopid.json'

def return_resolved_ids():
    return return_all_id()
def return_all_id():
    with open(all_shopid_json_file,'r') as f:
        d = json.load(f)
    return d.keys()
def return_unresolved_ids():
    return return_all_id()


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

def clean_df(df):
    """return : pandas.core.frame.DataFrame"""
    df.index = pd.DatetimeIndex(df.comm_time)
    df = df.sort_index()
    df = df[~(np.abs(df.com_per-df.com_per.mean())>(3*df.com_per.std()))]#清洗出三个标准差之外的数据,人均有关的计算用df2
    return df


def gather_y(df,dt):
    
    try:df2 = df[:dt]#限定在此范围
    except KeyError: return
    env = df2.env.mean()#环境评分
    taste = df2.taste.mean()#口味评分
    serv = df2.serv.mean()#服务评分
    total_rank = df2.total_rank.mean()#总评分
    has_pic_comm = df2[df2.has_picture].count()['serv']#带图评论数量
    total_comm = df2.count()['serv']#总评论数
    total_bus_rpl_comm = df2[df2.has_reply]['has_reply'].count()#有商家回复的评论
    sum_heart_num = df2.heart_num.sum()#点赞数
    total_hearted_comm = df2[df2.has_heart]['has_heart'].count()#有点赞的评论
    sum_recomm_num = df2.recomment_num.sum()#评论2数
    c = Counter(list(df2.com_id))
    dd = pd.DataFrame(c.items(),columns =['k','v'])
    laters = dd[dd['v']>1].v.count()#回头客数量
    #laters_freq = dd[dd['v']>1].v.sum()/max(1.0,float(laters))#回头客平均光顾次数
    laters_total = dd[dd['v']>1].v.sum()#回头客光顾总次数

    return {
    'env'+dt : env,#环境评分
    'taste'+dt : taste,#口味评分
    'serv'+dt : serv,#服务评分
    'total_rank'+dt : total_rank,#总评分
    'has_pic_comm'+dt : has_pic_comm,#带图评论数量
    'total_comm'+dt : total_comm,#总评论数
    'total_bus_rpl_comm'+dt : total_bus_rpl_comm,#有商家回复的评论
    'sum_heart_num'+dt : sum_heart_num,#点赞数
    'total_hearted_comm'+dt : total_hearted_comm,#有点赞的评论
    'sum_recomm_num'+dt : sum_recomm_num,#评论评论数
    'laters'+dt : laters,#回头客数量
    'laters_total'+dt :laters_total#回头客光顾总次数
    }



def gather_required_data(df):
    dts = dlist
    df = clean_df(df)
    df['has_reply'] = df.reply_content_len>0#增加一列来辅助，表示该评论有回复
    df['has_heart'] = df.heart_num>0#增加一列来辅助，表示该评论有回复
    #薛耕组变量
    dick_list = map(lambda x:gather_y(df,x),dts)
    
    init_d = {}
    for d in dick_list:
        init_d.update(d)
    init_d.update({'shop_id' : df['shop_id'][0]})
    return init_d

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
        DIR = '/Users/xuegeng/Spider_Workspace/crawler/shop_comment_parsed_xg2'
        ids_len = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])
        print "[=] %d shops solved"%ids_len
        print "[=] %d shops to solved"%(17900-ids_len)
    except:
        pass
        
ids = return_resolved_ids()[10000:]
total_df = ids_to_df(ids)
       
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
