package storm.starter.bolt;

import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

import java.util.Map;  

public class PostRankingsBolt extends ShellBolt implements IRichBolt {

    public PostRankingsBolt() {
      super("python", "postrankings.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("status"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }

}
