import storm
import requests
from uprspoutstream import UPRSpoutStream

import sys

class UPRSpoutTwitterDataStream(UPRSpoutStream):

    def emitTuple(self, msg):

        tweet = msg[0]
        vb = msg[1]
        tags = tweet['hashtags']
        id = tweet['id']
        storm.emit([tags[0], id, vb])

        if self.config['ui']['enabled']:
            self.updateUI(msg, vb)


    def updateUI(self, msg, vb):
        vbucket = msg[1]
        seqno = msg[2]
        high_seqno = msg[3]

        uiconf = self.config['ui']
        api = "http://{0}:{1}/spout".format(uiconf['ip'], uiconf['port'])
        path =  "/{0}/{1}/{2}".format(vbucket, seqno, high_seqno)
        url = api+path
        r = requests.get(url)

UPRSpoutTwitterDataStream().run()
