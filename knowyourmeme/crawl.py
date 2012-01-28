import gevent.monkey
gevent.monkey.patch_all()
import requests
from pyquery import PyQuery
import gevent
import re
import cPickle as pickle


def get_num_pages():
    url = 'http://knowyourmeme.com/memes/'
    content = requests.get(url).content
    print(url)
    pq = PyQuery(content)
    a = re.compile('/memes\?page=([0-9]+)')
    return max([int(a.search(x.get('href')).groups()[0]) for x in pq('a[href^="/memes?page="]')])


def get_meme_names():
    gs = []
    num_pages = max(min(get_num_pages(), 1000), 57)
    meme_names = set()

    def crawl(url):
        content = requests.get(url).content
        print(url)
        if content.find('Whoops! There are') > 0:
            return
        pq = PyQuery(content)
        meme_names.update(set(x.get('href') for x in pq('a[href^="/memes/"]')))

    for x in range(num_pages):
        g = gevent.Greenlet(crawl, 'http://knowyourmeme.com/memes?page=%d' % x)
        gs.append(g)
        g.start()

    for x in gs:
        x.join()
    return meme_names


def get_meme_photos(meme_names):
    meme_names = list(meme_names)
    
    # Find photo urls for memes
pq = PyQuery(requests.get('http://knowyourmeme.com/memes/wikipedia-donation-banner-captions').content)
sa = set(x.get('href') for x in pq('a[href^="/photos/"]'))
pq = PyQuery(requests.get('http://knowyourmeme.com/memes/teenage-mutant-ninja-noses').content)
sb = set(x.get('href') for x in pq('a[href^="/photos/"]'))
print(sa.intersection(sb))

def main():
    try:
        with open('meme_names.pkl') as fp:
            meme_names = pickle.load(fp)
    except IOError:
        meme_names = get_meme_names()
        with open('meme_names.pkl', 'w') as fp:
            meme_names = pickle.dump(meme_names, fp, -1)

