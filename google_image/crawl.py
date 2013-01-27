import requests
import urllib
import json
import glob
import os
import cPickle as pickle
import hadoopy_hbase


#hs.store(image, class_name, source, query=query, snippet=snippet, url=url)



def replay():
    datas = pickle.load(open('results.pkl'))
    hs = HBaseCrawlerStore()
    import imfeat
    for data in datas:
        for x in data['result']['items']:
            try:
                r = requests.get(x['link'], timeout=10)
                image = r.content
                if r.status_code != 200:
                    continue
            except (requests.exceptions.TooManyRedirects, requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                continue
            try:
                imfeat.image_fromstring(image)
            except IOError:
                continue
            query = data['query'].encode('utf-8')
            class_name = data['query'].encode('utf-8')[:-5].lower()
            source = 'google'
            snippet = x['snippet'].encode('utf-8')
            url = x['link'].encode('utf-8')
            hs.store(image, class_name, source, query=query, snippet=snippet, url=url)
            print(class_name)
        #return

#logos = []
#queries = [x.rstrip() + ' logo' for x in open('top_brands_businessweek.txt') if x.rstrip().lower() != 'nike']
#crawl('AIzaSyAkvR05pzUEDoRk6UpCnowdpYsyNxNcXKs', queries)  # ['nike logo']
replay()
