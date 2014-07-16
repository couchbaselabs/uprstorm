package storm.starter.spout;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.spout.ShellSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;

import java.util.Map;

public class UPRSpout extends ShellSpout implements IRichSpout {

  public UPRSpout(Integer index, Integer total) {
    super(new String[]{"python", "spoutloader.py",
          index.toString(), total.toString()});
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
   declarer.declare(new Fields("text", "id", "vbucket"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
   return null;
  }

}
