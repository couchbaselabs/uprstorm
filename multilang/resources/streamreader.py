import random

MAX_SEQNO = 0xFFFFFFFFFFFFFFFF
class StreamReader:

    def __init__(self, rest, start, end):
        self.streams = {}
        self.start = start
        self.end = end

        for vb in xrange(start, end):
            upr_client = rest.vbUprClient(vb)
            stream = upr_client.stream_req(vb, 0, 0, MAX_SEQNO, 0)
            self.streams[vb] = stream.response_gen()

    def response(self):
        while True:
            vb = random.randint(self.start, self.end -1)
            yield vb, self.streams[vb].next()

