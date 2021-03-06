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
        try:
            outs.append(func(r.content))
        except ValueError:
            return

    for url, func in url_funcs:
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
        yield  x['link'].encode('utf-8'), lambda y: {'source': 'google',
                                                     'snippet': x['snippet'].encode('utf-8'),
                                                     'query': query.encode('utf-8'),
                                                     'url': x['link'].encode('utf-8'),
                                                     'image': y}


def _street_view_crawl(lat, lon, api_key, incr=.0002, grid_radius=2, heading_delta=30, pitch=10, fov=60):
    def inner(scope):
        def inner(content):
            # Crude way to filter "The specified location could not be found." message
            if len(content) < 10000:
                raise ValueError
            scope['image'] = content
            return scope
        return inner
    for lat_shift in range(-grid_radius, grid_radius + 1):
        for lon_shift in range(-grid_radius, grid_radius + 1):
            for heading in range(0, 360, heading_delta):
                clat = lat + lat_shift * incr
                clon = lon + lon_shift * incr
                url = 'http://maps.googleapis.com/maps/api/streetview?size=640x640&location=%f,%%20%f&fov=%f&heading=%f&pitch=%f&sensor=false&key=%s' % (clat,
                                                                                                                                                         clon,
                                                                                                                                                         fov,
                                                                                                                                                         heading,
                                                                                                                                                         pitch,
                                                                                                                                                         api_key)

                yield url, inner({'source': 'streetview', 'latitude': str(clat), 'longitude': str(clon),
                                  'heading': str(heading), 'pitch': str(pitch), 'fov': str(fov), 'url': url})


def _flickr_crawl(api_key, api_secret, query=None, max_rows=500, min_upload_date=None, max_upload_date=None, page=None, has_geo=False, lat=None, lon=None, radius=None, one_per_owner=True, size='m', **kw):
    assert size in ('sq', 't', 's', 'q', 'm', 'n', 'z', 'c', 'l', 'o')
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
        res = flickr.photos_search(extras=extras,
                                   per_page=max_rows,
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
        owners = set()
        for photo in res.find('photos'):
            photo = dict(photo.items())
            try:
                out = {'source': 'flickr', 'url': photo['url_' + size]}
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
street_view_crawl = _crawl_wrap(_street_view_crawl)
flickr_crawl = _crawl_wrap(_flickr_crawl)
