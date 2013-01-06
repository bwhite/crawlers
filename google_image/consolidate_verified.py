import redis
import shutil
import os

good_entities = '../goodlogo/entity_images/'
r = redis.StrictRedis(port=6381, db=6)
key_to_path_db = redis.StrictRedis(port=6381, db=1)
for k in r.keys():
    print(k)
    try:
        user_data = r.hgetall(k)['user_data'] == 'true'
    except KeyError:
        continue
    if user_data:
        image = r.hgetall(k)['image']
        entity, fn = image.rsplit('/', 2)[1:]
        try:
            shutil.move(image, '%s/%s/%s' % (good_entities, entity, fn))
        except IOError:
            pass
    else:
        try:
            os.remove(image)
        except OSError:
            pass
r.flushdb()
