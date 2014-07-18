import os
import requests
from upr_bin_client import UprClient
from mc_bin_client import MemcachedClient

class Rest:
    def __init__(self, ip, port, username, password):
        self.ip = ip
        self.port = port
        self.username = username
        self.password = password
        api = "http://{0}:{1}/".format(self.ip, self.port)
        path = os.sep.join(["pools","default","buckets","default"])
        self.url = api + path
        self.__vbServerMap = self.vbServerMap()
        self.uprClientMap = {}
        self.mcdClientMap = {}

    def vbServerMap(self):
        r = requests.get(self.url, auth=(self.username, self.password))
        result = r.json()
        vbServerMap = result['vBucketServerMap']
        return vbServerMap

    def serverList(self):
        return self.__vbServerMap['serverList']

    def vbMap(self):
        return self.__vbServerMap['vBucketMap']

    def serverVb(self, vb):
        serverList = self.serverList()
        vbMap = self.vbMap()
        return serverList[vbMap[vb][0]]

    def vbUprClient(self, vb):
        server = self.serverVb(vb)
        if server in self.uprClientMap:
            client = self.uprClientMap[server]
        else:
            ip, port = server.split(':')
            client = UprClient(ip, int(port))
            client.open_producer("uprstorm"+str(vb))
            self.uprClientMap[server] = client

        return client

    def vbMcdClient(self, vb):
        server = self.serverVb(vb)
        if server in self.mcdClientMap:
            client = self.mcdClientMap[server]
        else:
            ip, port = server.split(':')
            client = MemcachedClient(ip, int(port))
            self.mcdClientMap[server] = client

        return client
