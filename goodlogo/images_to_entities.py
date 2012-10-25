import glob
import shutil
import os
import re


try:
    os.makedirs('entity_images')
except OSError:
    pass
for x in glob.glob('images/*'):
    fn = os.path.basename(x)
    year = re.search('(.*) \(([0-9]+)\)\.([^\.]+)', fn)
    if year:
        name, year, ext = year.groups()
        try:
            os.makedirs('entity_images/' + name)
        except OSError:
            pass
        out_path = os.path.join('entity_images', name, fn)
        shutil.copy(x, out_path)
    else:
        name, ext = re.search('(.*)\.(.*)', fn).groups()
        try:
            os.makedirs('entity_images/' + name)
        except OSError:
            pass
        shutil.copy(x, os.path.join('entity_images', name, fn))
