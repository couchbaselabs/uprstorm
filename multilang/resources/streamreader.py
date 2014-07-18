import random

MAX_SEQNO = 0xFFFFFFFFFFFFFFFF
class StreamReader:

    def __init__(self, rest, start, end):
        self.streams = {}
        self.start = start
        self.end = end
        self.rest = rest
        self.addAllStreams()

    def addStream(self, vb):
        try:
            upr_client = self.rest.vbUprClient(vb)
            start_seqno, uuid = self.rest.vbSeqnoUuid(vb)
            stream = upr_client.stream_req(vb, 0, start_seqno, MAX_SEQNO, uuid)
            self.streams[vb] = stream.response_gen()
        except:
            pass # this will retry

    def addAllStreams(self):
        for vb in xrange(self.start, self.end):
            self.addStream(vb)

    def reset(self):
        self.rest.updateVbMap()
        self.addAllStreams()

    def response(self):
        while True:
            vb = random.randint(self.start, self.end -1)

            if vb not in self.streams:
                self.addStream(vb)

            yield vb, self.streams[vb].next()

