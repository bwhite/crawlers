import gevent.monkey
gevent.monkey.patch_all()
import requests
from pyquery import PyQuery
import gevent
import re
import cPickle as pickle


PAGE_RE = re.compile('.*\?page=([0-9]+)')


def page_num(url):
    return int(PAGE_RE.search(url).groups()[0])


def get_num_pages():
    url = 'http://knowyourmeme.com/memes/'
    content = requests.get(url).content
    print(url)
    pq = PyQuery(content)
    return max([page_num(x.get('href')) for x in pq('a[href^="/memes?page="]')])


def get_meme_urls():
    num_pages = max(min(get_num_pages(), 1000), 57)
    meme_names = set()

    def crawl(url):
        content = requests.get(url).content
        print(url)
        if content.find('Whoops! There are') > 0:
            return
        pq = PyQuery(content)
        meme_names.update(set(x.get('href') for x in pq('a[href^="/memes/"]')))
    urls = ['http://knowyourmeme.com/memes?page=%d' % x for x in range(1, num_pages + 1)]
    batch_crawl(crawl, urls)
    return ['http://knowyourmeme.com%s' % x for x in meme_names]


def batch_crawl(crawl_func, urls):
    gs = []
    for url in urls:
        g = gevent.Greenlet(crawl_func, url)
        gs.append(g)
        g.start()

    for x in gs:
        x.join()


def get_meme_photos(meme_urls):
    meme_urls = list(meme_urls)
    photo_pages = {}  # [meme_url] = set of photo page urls
    photos = {}  # Set of photo urls

    def crawl_index(url):
        content = requests.get(url).content
        pq = PyQuery(content)
        print(url)
        num_pages = max(page_num(x.get('href')) for x in pq('a[href^="/memes/.*/photos\?page="]'))
        batch_crawl(crawl_photos, ['%s?page%d' % x for x in range(1, num_pages + 1)])

    def crawl_photos(url):
        content = requests.get(url).content
        pq = PyQuery(content)
        photo_pages[url] = set(x.get('href') for x in pq('a[href^="/photos/"]'))
        #photos[url] = set(x.get('href') for x in pq('a[href*="kym-cdn.com"]'))
    #batch_crawl(crawl, meme_urls)
    batch_crawl(crawl_index, [x + '/photos' for x in meme_urls])
    #http://knowyourmeme.com/memes/x-x-everywhere/photos?page=3
    
    return photos, photo_pages


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
    meme_photos = try_pickle_run('meme_photos.pkl', lambda : get_meme_photos(meme_urls[:10]))
    
main()
