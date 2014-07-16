import os 
import json
import time
import threading
from upr_bin_client import UprClient
from mc_bin_client import MemcachedClient as McdClient


MAX_SEQNO = 0xFFFFFFFFFFFFFFFF

class StreamReader(threading.Thread):

    def __init__(self, ip, port, vb, queue):
        super(StreamReader, self).__init__()

        self.ip = ip
        self.port = port
        self.upr_client = UprClient(self.ip, self.port)
        self.upr_client.open_producer("uprstorm"+str(vb))
        self.queue = queue
        self.vb = vb

    def getHighSeqno(self):
        mcd_client = McdClient(self.ip, self.port)
        resp = mcd_client.stats('vbucket-seqno')
        high_seqno = int(resp['vb_%s:high_seqno' % self.vb])
        return high_seqno

    def run(self):

        try:
            self.__run()

        except Exception as ex:
            self.queue.put({'err' : ex, 'vb' : self.vb})

    def __run(self):

        stream = self.upr_client.stream_req(self.vb, 0, 0, MAX_SEQNO, 0)
        high_seqno = self.getHighSeqno() 
        seqno = 0
        retries = 0

        while True:
            msg = stream.next_response(1)
            if msg:
                retries = 0

                if msg['opcode'] == 87:
                    seqno = msg['by_seqno']
                    if seqno > high_seqno:
                        high_seqno = self.getHighSeqno()

                    vbucket = msg['vbucket']
                    value = json.loads(msg['value'])
                    self.queue.put([value, vbucket, seqno, high_seqno])



