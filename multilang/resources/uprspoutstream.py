import sys
import yaml
import Queue
import storm
from streamreader import StreamReader
import threading


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

        self.MSG_Q = Queue.Queue()

        self.config = yaml.load(file('config.yaml', 'rb'))
        cbconf = self.config['couchbase']
        ip = cbconf['ip']
        port = int(cbconf['mc_port'])
        self.vbuckets = cbconf['vbuckets']
            
        start, end = computeVBRange(self.vbuckets)        
        for vb in range(start, end):
            streamReader = StreamReader(ip, port, vb, self.MSG_Q)
            streamReader.start()


    def nextTuple(self):
        msg = self.MSG_Q.get()
        if 'err' in msg:
            raise Exception(msg)
        self.emitTuple(msg)

    def emitTuple(self, msg):
        pass # override with emitter specific to use-case


