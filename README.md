uprstorm
========

Realtime processing of UPR streams with apache storm to detect trending topics using a [rolling count algorithm](http://www.michael-noll.com/blog/2013/01/18/implementing-real-time-trending-topics-in-storm/) .

![alt text](https://s3.amazonaws.com/tmcafeecouchbase/upr-storm+(1).jpg)


### Building on OSX
```
brew install maven
pip install pyaml
pip install requests
```

### Configure
Default config should be fine, if not check ``` multilang/resources/config.yaml ```

### Running
Easiest way to start topolgy is with maven
```bash
mvn compile exec:java -Dstorm.topology=storm.starter.UPRStormTopology
```

While topology is running, load some data
```
python data/loader.py 
```

### Monitor
UI is available for viewing stats and results. 
```
# Requires node and couchnode (TODO: update package.json with deps)
cd ui
node index.js 
http://localhost:3000
```

### TODO
more documentation


docker + fig deployement





