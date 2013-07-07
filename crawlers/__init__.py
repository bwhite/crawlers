import gevent
import requests
import urllib
import urllib2
import httplib
import json
import xml.parsers.expat
import xml.etree.ElementTree


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
        while len(gs) >= num_concurrent:
            gs = [g for g in gs if g.successful() and g.join() is None]
            gevent.sleep(.01)
        gs.append(gevent.spawn(_worker, url=url, func=func))
        for out in outs:
            yield out
        outs = []
    print('Pre join')
    gevent.joinall(gs)
    print('Post join')
    for out in outs:
        yield out


def _crawl_wrap(crawler):

    def inner(store, **kw):
        results = crawler(**kw)
        num_results = 0
        prev_output = set()
        for result in _batch_download(results):
            if result['url'] in prev_output:
                continue
            prev_output.add(result['url'])
            store(kw, **result)
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


def _flickr_crawl(api_key, api_secret, query=None, max_rows=500, min_upload_date=None, max_upload_date=None, page=None, has_geo=False, lat=None, lon=None, radius=None, one_per_owner=True, **kw):
    max_rows = max(1, min(max_rows, 500))
    import flickrapi
    flickr = flickrapi.FlickrAPI(api_key, api_secret)
    try:
        kw = {}
        if query is not None:
            kw['text'] = query
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
        print('Presearch')
        res = flickr.photos_search(extras=extras,
                                   per_page=max_rows,
                                   **kw)
        print('Postsearch')
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
        owners = set()
        for photo in res.find('photos'):
            photo = dict(photo.items())
            try:
                out = {'source': 'flickr', 'url': photo['url_m']}
            except KeyError:
                continue

            def _get_data(key):
                try:
                    out[key] = photo[key].encode('utf-8')
                except KeyError:
                    pass
            for key in ['title', 'tags', 'latitude', 'longitude',
                        'accuracy', 'dateupload', 'datetaken', 'owner']:
                _get_data(key)
            if one_per_owner and out['owner'] in owners:
                continue
            owners.add(out['owner'])

            def inner(scope):

                def post_download(content):
                    out = scope['out']
                    out['image'] = content
                    # Unavailable and other unrelated images are GIFs, skip them
                    if out['image'].startswith('GIF87'):
                        raise ValueError
                    return out
                return post_download
            yield out['url'], inner(dict(locals()))


google_crawl = _crawl_wrap(_google_crawl)
flickr_crawl = _crawl_wrap(_flickr_crawl)
