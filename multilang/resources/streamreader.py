import random
from upr_bin_client import UprClient

MAX_SEQNO = 0xFFFFFFFFFFFFFFFF
class StreamReader:

    def __init__(self, ip, port, start, end):
        self.streams = {}
        self.start = start
        self.end = end
        upr_client = UprClient(ip, port)
        upr_client.open_producer("uprstorm"+str(start))

        for vb in range(start, end):
            stream = upr_client.stream_req(vb, 0, 0, MAX_SEQNO, 0)
            self.streams[vb] = stream.response_gen()

    def response(self):
        while True:
            vb = random.randint(self.start, self.end -1)
            yield vb, self.streams[vb].next()

