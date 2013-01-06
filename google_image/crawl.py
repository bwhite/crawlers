import requests
import urllib
import json
import glob
import os
import cPickle as pickle


def get(api_key, query):
    query = urllib.quote(query)
    return json.loads(requests.get('https://www.googleapis.com/customsearch/v1?key=%s&cx=011323180791009162744:boa0ofeir5k&q=%s&searchType=image&imgType=clipart' % query, verify=False).content)

db = pickle.load(open('entity_google.pkl'))
ct = 0
for x in sorted(glob.glob('../goodlogo/entity_images/*')):
    x = os.path.basename(x)
    if x in db:
        continue
    ct += 1
    if ct >= 100:
        break
    #print(x)
    l = get('%s logo' % x)
    db[x] = l
    print(l)
    pickle.dump(db, open('entity_google.pkl', 'w'), -1)
print(ct)
