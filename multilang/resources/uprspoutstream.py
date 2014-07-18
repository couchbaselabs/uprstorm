import sys
import json
import yaml
import storm
from streamreader import StreamReader

CONFIG = yaml.load(file('config.yaml', 'rb'))

def computeVBRange(vbuckets):
    start = 0
    end = vbuckets

    if len(sys.argv) == 3:
        total = int(sys.argv[2])
        vbs_per_spout = vbuckets/total
        start = int(sys.argv[1])*vbs_per_spout
        end = start + vbs_per_spout

    return start, end

class UPRSpoutStream(storm.Spout):

    def initialize(self, conf, context):

        cbconf = CONFIG['couchbase']
        ip = cbconf['ip']
        port = int(cbconf['mc_port'])
        vbuckets = cbconf['vbuckets']
        start, end = computeVBRange(vbuckets)
        self.reader = StreamReader(ip, port, start, end)

    def nextTuple(self):
        vb, msg = self.reader.response().next()
        if msg and msg['opcode'] == 87:
            tweet = json.loads(msg['value'])
            tags = tweet['hashtags']
            id = tweet['id']
            storm.emit([tags[0], id, vb])

UPRSpoutStream().run()
