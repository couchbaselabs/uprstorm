import os
import sys
import ast
import json
import time
import yaml
import zlib

CTXDIR = os.path.dirname(os.path.abspath(__file__))
LIBDIR = os.sep.join([CTXDIR, "..", "multilang", "resources"])
DATA_FILE = os.sep.join([CTXDIR, 'sample_dataset'])
CONF_FILE = os.sep.join([LIBDIR, "config.yaml"])
sys.path = sys.path + [LIBDIR]
from  rest import Rest


config = yaml.load(file(CONF_FILE, 'rb'))
ip = config['couchbase']['ip']
port= int(config['couchbase']['port'])
username = config['couchbase']['username']
password = config['couchbase']['password']
num_vbuckets = config['couchbase']['vbuckets']

rest = Rest(ip, int(port), username, password)


def vbucket(key):
    return (((zlib.crc32(key)) >> 16) & 0x7fff) & (num_vbuckets - 1)

while True:

    with open(DATA_FILE) as f:

        msg = ""
        i = 0
        while True:
            line = f.readline()[0:-1]
            if not line: break

            if line == "---eot---":
                i += 1
                tweet = ast.literal_eval(msg)
                key = "uprstorm_"+str(i)
                vb = vbucket(key)
                client = rest.vbMcdClient(vb)
                client.vbucket_count = num_vbuckets
                client.set(key, 0, 0, json.dumps(tweet))
                msg = ""
                continue
            msg = msg + line + "\n"
