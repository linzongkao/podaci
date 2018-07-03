# -*- coding: utf-8 -*-
"""
Created on Wed Mar 21 15:57:20 2018

@author: ldh
"""


# update_data.py

import os
import requests
import datetime
import tempfile
import shutil
import tarfile

def update_bundle(data_bundle_path=None):
    if data_bundle_path is None:
        data_bundle_path = os.path.abspath(os.path.expanduser('~/.mkdata/bundle'))
        
    day = datetime.date.today()
    tmp = os.path.join(tempfile.gettempdir(), 'rq.bundle')
    
    while True:
        url = 'http://7xjci3.com1.z0.glb.clouddn.com/bundles_v3/rqbundle_%04d%02d%02d.tar.bz2' % (
            day.year, day.month, day.day)
        r = requests.get(url, stream=True)
        if r.status_code != 200:
            day = day - datetime.timedelta(days=1)
            continue

        out = open(tmp, 'wb')
        total_length = int(r.headers.get('content-length'))

        for data in r.iter_content(chunk_size=8192):
            out.write(data)

        out.close()

    shutil.rmtree(data_bundle_path, ignore_errors=True)
    os.makedirs(data_bundle_path)
    tar = tarfile.open(tmp, 'r:bz2')
    tar.extractall(data_bundle_path)
    tar.close()
    os.remove(tmp)
    
if __name__ == '__main__':
    update_bundle()


