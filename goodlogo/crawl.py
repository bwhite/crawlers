import gevent.monkey
gevent.monkey.patch_all()
import requests
from pyquery import PyQuery
import gevent
import re
import cPickle as pickle
from shove import Shove
import os
import random
import crawlers
from lxml import etree
import lxml.html
import json
import base64

def crawl_az():
    az = crawlers.get_url('http://www.goodlogo.com/a-z')
    return list(re.findall(r'<a href="(/extended[^"]+)" .*>([^<]+)</a>', az))


def parse_extended(az):
    crawlers.batch_crawl(lambda x: crawlers.get_url('http://www.goodlogo.com' + x[0]), az)

    def get_main_logo(data):
        path, name = data
        urls = etree.HTML(crawlers.get_url('http://www.goodlogo.com' + path)).xpath('//img[@longdesc and @width > 10 and @height > 10]')
        urls = [re.search('(/images/logos/[^/]+\..*)', u.get('src')) for u in urls]
        try:
            url, = [u.groups(1)[0] for u in urls if u]
        except ValueError:
            print(path)
        logo_url = 'http://www.goodlogo.com' + url
        logo = crawlers.get_url(logo_url)
        name = name.decode('ascii', 'ignore')
        try:
            os.makedirs('logos')
        except OSError:
            pickle.dump({'name': name, 'logo_url': logo_url, 'logo': logo}, open('logos/%s.pkl' % name, 'w'))
        try:
            os.makedirs('images')
        except OSError:
            open('images/%s.%s' % (name, logo_url.rsplit('.', 1)[-1]), 'w').write(logo)
    crawlers.batch_crawl(get_main_logo, az)


def main():
    crawlers.setup_db('db')
    az = crawlers.try_pickle_run('az.pkl', crawl_az)
    parse_extended(az)


if __name__ == '__main__':
    main()
