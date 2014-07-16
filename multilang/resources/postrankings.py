
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import storm
import yaml
import requests

class PostRankingsBolt(storm.BasicBolt):

    def initialize(self, conf, context):
        self.config = yaml.load(file('config.yaml', 'rb'))

    def process(self, tup):
        rankings = tup.values[0]

        rank = 0
        for ranking in rankings:
            topic = ranking.keys()[0]
            count = ranking[topic][0]
            id = ranking[topic][1]

            if self.config['ui']['enabled']:
                response = self.postRankings(topic, rank, count, id)
                storm.emit([response])

            rank += 1

    def postRankings(self, topic, rank, count, id):
        uiconf = self.config['ui']
        api = "http://{0}:{1}/storm".format(uiconf['ip'], uiconf['port'])
        path =  "/{0}/{1}/{2}/{3}".format(topic, rank, count, id) 
        url = api+path
        r = requests.get(url)
        return r.text


PostRankingsBolt().run()
