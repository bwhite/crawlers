from fabric.api import run, sudo, put, get
from fabric.context_managers import cd, settings
import time
import os


def crawl():
    t = '%f' % time.time()
    working_dir = 'deploys/deploy-%s' % t
    try:
        run('mkdir -p %s' % working_dir)
        with cd(working_dir):
            run('git clone git@github.com:bwhite/crawlers.git')
            with cd('crawlers/knowyourmeme'):
                run('python crawl.py')
                get('meme_photos.pkl', 'meme_photos.pkl')
                get('meme_urls.pkl', 'meme_urls.pkl')
    finally:
        run('rm -fr %s' % working_dir)
            
            
 
