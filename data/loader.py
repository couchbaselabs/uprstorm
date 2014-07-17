import os
import sys
import ast
import json
import time
import yaml

CTXDIR = os.path.dirname(os.path.abspath(__file__))
LIBDIR = os.sep.join([CTXDIR, "..", "multilang", "resources"])
DATA_FILE = os.sep.join([CTXDIR, 'sample_dataset'])
CONF_FILE = os.sep.join([LIBDIR, "config.yaml"])
sys.path = sys.path + [LIBDIR]
from mc_bin_client import MemcachedClient


config = yaml.load(file(CONF_FILE, 'rb'))
ip = config['couchbase']['ip']
port= int(config['couchbase']['mc_port'])
client = MemcachedClient(ip, port)
client.vbucket_count = int(config['couchbase']['vbuckets'])

ops = 300
while True:

    with open(DATA_FILE) as f:

        msg = ""
        op = 0
        while True:
            line = f.readline()[0:-1]
            if not line: break

            if line == "---eot---":
                tweet = ast.literal_eval(msg)
                key = str(tweet['id'])
                client.set(key, 0, 0, json.dumps(tweet))
                op += 1
                if op == ops:
                    time.sleep(1)
                    op = 0
                msg = ""
                continue
            msg = msg + line + "\n"
