# -*- coding: utf-8 -*-
import requests,re,json,pickle,os
from lxml import etree
from bs4 import BeautifulSoup
from CONSTANTS import HEADER2
from CrawlFunctions import getSoup,getEtreeHtml,getSoup
from multiprocessing.dummy import Lock,Pool

lock = Lock()

with open('/Users/xuegeng/Desktop/crawler/shop_comment_pages_crawled27.json','r') as f:
        origin_is_page_crawled = json.load(f)
#with open('/Users/xuegeng/Desktop/crawler/shop_comment_pages_crawled.json','r') as f:
#        pages_to_crawl = json.load(f)
pages_to_crawl = origin_is_page_crawled.copy()

for it in origin_is_page_crawled.iteritems():
    if it[1]:
        del pages_to_crawl[it[0]]

#for url in page_no_crawled.keys():
#    url =  url.split('/')
#    dir_name = url[-2]+'_'+url[-1]  
#    dir_path = '/Users/xuegeng/Desktop/crawler/htmls/'+dir_name
#    os.makedirs(dir_path)
#    print dir_path
def describe():
    global origin_is_page_crawled
    try:
        count = 0
        for i in origin_is_page_crawled.iterkeys():
            if origin_is_page_crawled[i]:
                count += 1
        print "[=] %d pages crawled"%count
        print "[=] %d pages to crawl"%(len(origin_is_page_crawled)-count)
    except:
        pass
describe()
def getFilePath(url):
    
    url1 =  url.split('/')            
    dir_name = url1[-3]+'_'+url1[-2]
    url2 = url.split('=')
    page_num = url2[-1]
    dir_path = '/Users/xuegeng/Desktop/crawler/htmls' + os.sep+dir_name +os.sep+dir_name+'_p'+page_num+'.html'
    return dir_path
    
def getHtmlText(url):
    '''return text-form html'''
    try:
        html = requests.get(url, headers = HEADER2, timeout = 5)
        html.encoding = 'utf-8'
        return html.text
    except:
        print "[-] Fail to get " + url
        return None        
def getMaxPageNum(soup):
    '''return max page number of the result'''
    p = soup.find(class_ = "Pages")
    try:
        p_text = p.get_text()
        digits = list(filter(lambda x:x.isdigit(),p_text.split('\n')))
        max_page_num = max(map(int,digits))
    except AttributeError:
        max_page_num = 0
    except ValueError:
        max_page_num = 1
    return max_page_num

def isBanned(text):
    
    """make sure if a thread is caught as spider, if so , change cookie"""
    
    chptcha_url = 'http://www.dianping.com/alpaca/captcha.jpg'
    if chptcha_url in text:
        return True
    return False
    
def isFirstCommentPage(text):
    
    '''返回这个页面是不是评论的第一页'''
    
    soup = getSoup(text)
    max_page_num = getMaxPageNum(soup)
    return max_page_num <= 1
  
def writeHtml(file_name,text):
    '''将html文件写入本地'''
    with open(file_name,'w') as f:
        f.write(text.encode('utf-8'))

def generatePageList(url,num):
    
    r = map(lambda x:x+1,range(1,num))
    l = [url[:-1]+str(i) for i in r]
    return l
          
def saveHtml(html_text,url):
    
    global origin_is_page_crawled
    
    #从url提取出文件路径名
    file_path = getFilePath(url)
    #如果text里面有验证码：return
    if isBanned(html_text):
        return
        
    #如果没有验证码且不为空，则存储，修改origin_is_page_crawled的布尔
    try:
        writeHtml(file_path,html_text)
        origin_is_page_crawled[url] = True
        print "[+] Saving %s succeed!"%file_path
    except:
        origin_is_page_crawled[url] = False
        print "[-] Saving %s Failed!"%file_path
        
    #如果是第一页评论，修改origin_is_page_crawled的Key
    try:
        if url.split('=')[-1] != '1':return #如果这不是第一页，那么不用管加页的事
        soup = getSoup(html_text)
        max_page_num = getMaxPageNum(soup)
        if max_page_num > 1:
            l = generatePageList(url,max_page_num)
            print "[+] Appending %d pages"%len(l)
            for u in l:
                origin_is_page_crawled[u] = False
    except:
        origin_is_page_crawled[url] = False
        print "[-] Appending pages Failed"
        
def mergeIntoTheResult(page_url):
    global origin_is_page_crawled,lock
    file_path = getFilePath(page_url)
    
    if os.path.exists(file_path):
        origin_is_page_crawled[page_url] = True
        return 
        
    page_text = getHtmlText(page_url)
    if page_text:
        with lock:
            saveHtml(page_text,page_url)
            
def processMerging():
    '''to have process handle shop url crawling'''
    global pages_to_crawl,origin_is_page_crawled,shop_urls
    pool = Pool(processes = 25)
    results = pool.map(mergeIntoTheResult, pages_to_crawl.keys())
    pool.close()
    pool.join()
    with open('/Users/xuegeng/Desktop/crawler/shop_comment_pages_crawled28.json','w') as f:
        json.dump(origin_is_page_crawled,f)



processMerging()
describe()
with open('/Users/xuegeng/Desktop/crawler/shop_comment_pages_crawled28.json','w') as f:
        json.dump(origin_is_page_crawled,f)

