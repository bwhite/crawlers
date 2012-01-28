import gevent.monkey
gevent.monkey.patch_all()
import requests
from pyquery import PyQuery
import gevent
import re
import cPickle as pickle
from shove import Shove
import os
import urllib

DB = Shove('sqlite:///%s' % (os.path.expanduser('~/crawl_cache.db')), compress=True)
PAGE_RE = re.compile('.*\?page=([0-9]+)')


def get_url(url):
    try:
        return DB[url]
    except KeyError:
        r = requests.get(urllib.unquote(url))
        if r.status_code == 200:
            DB[url] = r.content
            return DB[url]
        else:
            print('Url[%s] Gave Code[%d]' % (url, r.status_code))
            return r.content


def page_num(url):
    return int(PAGE_RE.search(url).groups()[0])


def get_num_pages():
    url = 'http://knowyourmeme.com/memes/'
    content = get_url(url)
    pq = PyQuery(content)
    return max([page_num(x.get('href')) for x in pq('a[href^="/memes?page="]')])


def get_meme_urls():
    num_pages = max(min(get_num_pages(), 1000), 57)
    meme_names = set()

    def crawl(url):
        content = get_url(url)
        if content.find('Whoops! There are') > 0:
            return
        pq = PyQuery(content)
        meme_names.update(set(x.get('href') for x in pq('a[href^="/memes/"]')))
    urls = ['http://knowyourmeme.com/memes?page=%d' % x for x in range(1, num_pages + 1)]
    batch_crawl(crawl, urls)
    return ['http://knowyourmeme.com%s' % x for x in meme_names]


def batch_crawl(crawl_func, datas, num_connections=30):
    gs = []
    for data in datas:
        g = gevent.spawn(crawl_func, data)
        gs.append(g)
        if len(gs) >= num_connections:
            gevent.joinall(gs)
            gs = []
    gevent.joinall(gs)


def get_meme_photos(meme_urls):
    meme_urls = list(meme_urls)
    photo_pages = {}  # [meme_url] = set of photo page urls

    def crawl_index(url):
        content = get_url(url)
        pq = PyQuery(content)
        num_pages = max([page_num(x.get('href')) for x in pq('a[href*="/photos?page="]')] + [1])
        batch_crawl(crawl_photos, [(url, '%s?page=%d' % (url, x)) for x in range(1, num_pages + 1)])

    def crawl_photos(urls):
        parent_url, url = urls
        content = get_url(url)
        pq = PyQuery(content)
        photo_pages.setdefault(parent_url, set()).update(set('http://knowyourmeme.com' + x.get('href') for x in pq('a[class^="photo"]')))

    # This gets a list of all of the "photo pages"
    batch_crawl(crawl_index, [x + '/photos' for x in meme_urls])
    return photo_pages


def get_meme_photo_images(photo_page_urls):
    images = {}  # Set of photo urls

    def crawl_photo_image(urls):
        parent_url, url = urls
        content = get_url(url)
        pq = PyQuery(content)
        images.setdefault(parent_url, set()).add(pq('img[class="centered_photo"]')[0].get('src'))

    url_pairs = sum(([(x, z) for z in y] for x, y in photo_page_urls.items()), [])
    batch_crawl(crawl_photo_image, url_pairs)
    return images


def try_pickle_run(fn, func):
    try:
        with open(fn) as fp:
            return pickle.load(fp)
    except IOError:
        out = func()
        with open(fn, 'w') as fp:
            pickle.dump(out, fp, -1)
        return out


def main():
    meme_urls = try_pickle_run('meme_urls.pkl', get_meme_urls)
    meme_photos = try_pickle_run('meme_photos.pkl', lambda : get_meme_photos(meme_urls))
    #meme_photos = dict(meme_photos.items()[:10])
    #meme_photo_images = try_pickle_run('meme_photo_images.pkl', lambda : get_meme_photo_images(meme_photos))
    DB.sync()
    
main()
