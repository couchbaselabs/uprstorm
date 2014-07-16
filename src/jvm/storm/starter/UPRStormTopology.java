/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import backtype.storm.Config;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.tuple.Fields;
import storm.starter.spout.UPRSpout;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.bolt.PostRankingsBolt;
import storm.starter.util.StormRunner;

import java.util.Map;
import org.apache.log4j.Logger;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class UPRStormTopology{

  private static final Logger LOG = Logger.getLogger(UPRStormTopology.class);
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 3600;
  private static final int TOP_N = 5;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  public UPRStormTopology() throws InterruptedException {
    builder = new TopologyBuilder();
    topologyName = "slidingWindowCounts";
    topologyConfig = createTopologyConfiguration();
    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

    wireTopology();
  }

  private static Config createTopologyConfiguration() {
    Config conf = new Config();
    conf.setDebug(true);
    return conf;
  }


  private String[] createSpouts(Integer numberOfSpouts){

    String[] ids = new String[numberOfSpouts]; 
    for (Integer i = 0; i < numberOfSpouts; i++){
        ids[i] = "uprSpout"+i;
        builder.setSpout(ids[i], new UPRSpout(i, numberOfSpouts), 1);
    }
    return ids;
  }


  private void wireTopology() throws InterruptedException {
    String counterId = "counter";
    String splitId = "split";
    String filterId = "filter";
    String intermediateRankerId = "intermediateRanker";
    String totalRankerId = "finalRanker";
    String postRankerId = "postRanker";

    // upr spouts to emit words 
    String[] spoutIds = createSpouts(4);


    // map word count bolt to spouts 
    BoltDeclarer counterBolt = builder.setBolt(counterId, new RollingCountBolt(15, 5), 8);
    for (String id: spoutIds){
        counterBolt.fieldsGrouping(id, new Fields("vbucket"));
    }

    // intermediate rankings
    builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N, 3), 4).fieldsGrouping(counterId, new Fields("obj"));

    // final rankings
    builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N, 3)).globalGrouping(intermediateRankerId);

    // update UI
    builder.setBolt(postRankerId, new PostRankingsBolt(), 1).shuffleGrouping(totalRankerId);
  }

  public void run() throws InterruptedException {
    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
  }

  public static void main(String[] args) throws Exception {
    new UPRStormTopology().run();
  }

}
