import gevent
import requests
import urllib
import urllib2
import httplib
import json
import glob
import os
import cPickle as pickle
import hadoopy_hbase
import xml.parsers.expat
import xml.etree.ElementTree
import os
import sys
import hashlib


class HBaseCrawlerStore(object):

    def __init__(self, hb, row_prefix):
        self.hb = hb
        self.table = 'images'
        self.row_prefix = row_prefix
        self.random_bytes = 10

    def store(self, image, class_name, source, query, **kw):
        cols = []
        md5 = lambda x: hashlib.md5(x).digest()
        add_col = lambda x, y: cols.append(hadoopy_hbase.Mutation(column=x, value=y))
        add_col('data:image', image)
        add_col('meta:class', class_name)
        add_col('meta:query', query)
        add_col('meta:source', source)
        add_col('hash:md5', md5(image))
        for x, y in kw.items():
            add_col('meta:' + x, y)
        row = self.row_prefix + os.urandom(self.random_bytes)
        self.hb.mutateRow(self.table, row, cols)


def _verify_image(image_binary):
    import imfeat
    try:
        imfeat.image_fromstring(image_binary)
    except IOError:
        return False
    return True


def _batch_download(url_funcs, num_concurrent=50):
    gs = []
    outs = []

    def _worker(url, func):
        try:
            r = requests.get(url, timeout=3)
        except (requests.exceptions.TooManyRedirects, requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            return
        if r.status_code != 200:
            return
        outs.append(func(r.content))

    for url, func in url_funcs:
        print(url)
        while len(gs) >= num_concurrent:
            gs = [g for g in gs if g.successful() and g.join() is None]
            gevent.sleep(.01)
        gs.append(gevent.spawn(_worker, url=url, func=func))
        for out in outs:
            yield out
        outs = []
    gevent.joinall(gs)
    for out in outs:
        yield out


def _crawl_wrap(crawler):

    def inner(store, class_name, query=None, *args, **kw):
        query = query if query is not None else class_name
        results = crawler(query, *args, **kw)
        num_results = 0
        prev_output = set()
        for result in _batch_download(results):
            if result['url'] in prev_output:
                continue
            prev_output.add(result['url'])
            store.store(class_name=class_name, query=query, **result)
            num_results += 1
        return num_results
    return inner


def _google_crawl(query, api_key):
    query = urllib.quote(query)
    result = json.loads(requests.get('https://www.googleapis.com/customsearch/v1?key=%s&cx=011323180791009162744:boa0ofeir5k&q=%s&searchType=image&imgType=clipart' % (api_key, query), verify=False).content)
    for x in result['items']:
        try:
            r = requests.get(x['link'], timeout=1)
            image = r.content
            if r.status_code != 200:
                continue
        except (requests.exceptions.TooManyRedirects, requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            continue
        if not _verify_image(image):
            continue
        yield {'source': 'google',
               'snippet': x['snippet'].encode('utf-8'),
               'url': x['link'].encode('utf-8'),
               'query': query.encode('utf-8')}


def _flickr_crawl(query, api_key, api_secret, min_upload_date=None, max_upload_date=None, page=None, has_geo=False, lat=None, lon=None, radius=None):
    import flickrapi
    flickr = flickrapi.FlickrAPI(api_key, api_secret)
    prev_output = set()
    try:
        kw = {}
        if min_upload_date is not None:
            kw['min_upload_date'] = min_upload_date
        if max_upload_date is not None:
            kw['max_upload_date'] = max_upload_date
        if page is not None:
            kw['page'] = page
        if has_geo:
            kw['has_geo'] = 1
        if lat is not None and lon is not None:
            kw['lat'] = lat
            kw['lon'] = lon
            if radius is not None:
                kw['radius'] = str(radius)
        extras = 'description,license,date_upload,date_taken,owner_name,icon_server,original_format,last_update,geo,tags,machine_tags,o_dims,views,media,path_alias,url_sq,url_t,url_s,url_q,url_m,url_n,url_z,url_c,url_l,url_o'
        print(query)
        res = flickr.photos_search(text=query,
                                   extras=extras,
                                   per_page=500,
                                   **kw)
    except (httplib.BadStatusLine,
            flickrapi.exceptions.FlickrError,
            xml.parsers.expat.ExpatError,
            xml.etree.ElementTree.ParseError,
            urllib2.URLError,
            urllib2.HTTPError), e:
        # TODO: Handle exceptions
        print(e)
        return
    else:
        for photo in res.find('photos'):
            photo = dict(photo.items())
            print(photo)
            try:
                if photo['url_m'] in prev_output:
                    continue
                prev_output.add(photo['url_m'])
                print(photo['url_m'])
                out = {'source': 'flickr', 'url': photo['url_m']}
            except KeyError:
                continue

            def _get_data(key):
                try:
                    out[key] = photo[key].encode('utf-8')
                except KeyError:
                    pass
            print(photo)
            for key in ['title', 'tags', 'latitude', 'longitude',
                        'accuracy', 'dateupload', 'datetaken']:
                _get_data(key)

            def post_download(content):
                out['image'] = content
                # Unavailable and other unrelated images are GIFs, skip them
                if out['image'].startswith('GIF87'):
                    raise ValueError
                return out
            yield out['url'], post_download


google_crawl = _crawl_wrap(_google_crawl)
flickr_crawl = _crawl_wrap(_flickr_crawl)
