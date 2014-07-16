import ast
import time
from couchbase import Couchbase
cb = Couchbase.connect(bucket='default')
ops = 300

while True:

    with open('sample_dataset') as f:

        msg = ""
        op = 0
        while True:
            line = f.readline()[0:-1]
            if not line: break

            if line == "---eot---":
                tweet = ast.literal_eval(msg)
                key = str(tweet['id'])
                cb.set(key, tweet)
                op += 1
                if op == ops:
                    time.sleep(1)
                    op = 0
                msg = ""
                continue
            msg = msg + line + "\n"
