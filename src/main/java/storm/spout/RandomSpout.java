package storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import storm.bean.LogMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomSpout extends BaseRichSpout {
   private SpoutOutputCollector outputCollector;
   private TopologyContext topologyContex;
   private List<LogMessage>list;
    public RandomSpout() {
        super();
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector outputCollector) {
        this.topologyContex=topologyContext;
        this.outputCollector=outputCollector;
        list = new ArrayList();
        list.add(new LogMessage(1,"http://www.itcast.cn/product?id=1002",
                "http://www.itcast.cn/","yanjingke"));
        list.add(new LogMessage(1,"http://www.itcast.cn/product?id=1002",
                "http://www.itcast.cn/","yanjingke"));
        list.add(new LogMessage(1,"http://www.itcast.cn/product?id=1002",
                "http://www.itcast.cn/","yanjingke"));
        list.add(new LogMessage(1,"http://www.itcast.cn/product?id=1002",
                "http://www.itcast.cn/","yanjingke"));
    }

    @Override
    public void nextTuple() {
        final Random random=new Random();
        LogMessage meg=list.get(random.nextInt(4));
        outputCollector.emit(new Values(new Gson().toJson(meg)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("paymentInfo"));
    }
}
