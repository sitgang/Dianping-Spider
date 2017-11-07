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


delta = datetime.timedelta(days=32)
down_limit = datetime.datetime(2016,11,1)
up_limit = datetime.datetime(2017,5,1)

d = down_limit
dlist=[]
while d <= up_limit:
    dlist.append(d)
    d+=delta
dlist=map(lambda x:x.strftime('%Y-%m'),dlist)

client = MongoClient()
clct = client.foodstore.bjfoodstore
mongo_lock = Lock()
format_json_filename = '/Users/xuegeng/Spider_Workspace/crawler/shop_comment_parsed_sh2/%s.json'
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
    
    try:df = df[dt]#限定在此范围
    except KeyError: return
    good_comm = df[df['total_rank']>=4]['serv'].count()#好评数
    mid_comm = df[df['total_rank']==3]['serv'].count()#中评数
    bad_comm = df[df['total_rank']<=2]['serv'].count()#差评数

    comm = df['serv'].count()#评论数
    comm_rpy = df[df['has_reply']]['serv'].count()#回复数
    
    good_comm_rpy = df[(df['total_rank']>=4)&df['has_reply']]['serv'].count()#好评回复数
    mid_comm_rpy = df[(df['total_rank']==3)&df['has_reply']]['serv'].count()#中评回复数
    bad_comm_rpy = df[(df['total_rank']<=2)&df['has_reply']]['serv'].count()#差评回复数
    
    comm_time = df[df['has_reply']]['comm_time'].values.astype('datetime64[m]')
    bus_rep_time = df[df['has_reply']]['bus_rep_time'].values.astype('datetime64[m]')
    try:delay_amount = sum(bus_rep_time - comm_time)/np.timedelta64(1, 'm')#延迟总量
    except (KeyError,TypeError):  delay_amount = 0
    reply_content_len = df[df['has_reply']]['reply_content_len'].sum()#回复长度
    good_reply_content_len = df[(df['total_rank']>=4)&df['has_reply']]['reply_content_len'].sum()#好评回复长度
    mid_reply_content_len = df[(df['total_rank']==3)&df['has_reply']]['reply_content_len'].sum()#中评回复长度
    bad_reply_content_len = df[(df['total_rank']<=2)&df['has_reply']]['reply_content_len'].sum()#差评回复长度
    

    return {
    'good_comm'+dt : good_comm,#好评数
    'mid_comm'+dt : mid_comm,#中评数
    'bad_comm'+dt : bad_comm,#差评数
    'comm'+dt : comm,#评论数
    'comm_rpy'+dt : comm_rpy,#回复数
    'good_comm_rpy'+dt : good_comm_rpy,#好评回复数
    'mid_comm_rpy'+dt : mid_comm_rpy,#中评回复数
    'bad_comm_rpy'+dt : bad_comm_rpy,#差评回复数
    'delay_amount'+dt : delay_amount,#延迟总量
    'reply_content_len'+dt : reply_content_len,#回复长度
    'good_reply_content_len'+dt : good_reply_content_len,#好评回复长度
    'mid_reply_content_len'+dt : mid_reply_content_len,#中评回复长度
    'bad_reply_content_len'+dt : bad_reply_content_len,#差评回复长度
    }



def gather_required_data(df):
    dts = dlist
    df = clean_df(df)
    df['has_reply'] = df.reply_content_len>0#增加一列来辅助，表示该评论有回复
    #孙航组变量
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
        DIR = '/Users/xuegeng/Spider_Workspace/crawler/shop_comment_parsed_sh2'
        ids_len = len([name for name in os.listdir(DIR) if os.path.isfile(os.path.join(DIR, name))])
        print "[=] %d shops solved"%ids_len
        print "[=] %d shops to solved"%(17900-ids_len)
    except:
        pass
        
ids = return_resolved_ids()[:10]
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


#try:
#    process_worker()
#except KeyboardInterrupt:
#    print "HAHAHAHAHAHAHAHA"
for ii in ids:
    worker_job(ii)



#data = gather_required_data(id_to_df("68717373"))#用来检测

#in_fp = '/Users/xuegeng/Spider_Workspace/crawler/shop_datas.json'
#id_solved_fp = '/Users/xuegeng/Spider_Workspace/crawler/is_id_solved.json'
#with open(in_fp,'w') as f:
#            json.dump([],f)
#with open(id_solved_fp,'w') as f:
#            json.dump(dict(map(lambda x:(x,False),return_all_id())),f)
