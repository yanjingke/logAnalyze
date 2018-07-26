package storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.MessageFilterBolt;
import storm.bolt.ProcessMessageBolt;
import storm.spout.RandomSpout;

public class LogAnalyzeToplogyMain {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder topologyBuilder=new  TopologyBuilder();
        topologyBuilder.setSpout("kafkaSpout",new RandomSpout(),2)  ;
        topologyBuilder.setBolt("MessageFilter-bolt",new MessageFilterBolt(),3).shuffleGrouping("kafkaSpout");
        topologyBuilder.setBolt("ProcessMessage-bolt",new ProcessMessageBolt(),2).fieldsGrouping("MessageFilter-bolt", new Fields("type"));
        Config topologConf = new Config();
        if (args != null && args.length > 0) {
            topologConf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], topologConf, topologyBuilder.createTopology());
        } else {
            topologConf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("LogAnalyzeTopologyMain", topologConf, topologyBuilder.createTopology());
            Utils.sleep(10000000);
            cluster.shutdown();
        }
    }
}
