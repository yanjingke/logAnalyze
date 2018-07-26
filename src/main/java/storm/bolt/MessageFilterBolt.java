package storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.bean.LogMessage;
import storm.util.LogAnalyzeHandler;

public class MessageFilterBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String line=tuple.getString(0);
        LogMessage logMessage = LogAnalyzeHandler.parser(line);
        if (logMessage==null||!LogAnalyzeHandler.isValidType(logMessage.getType())){
                return;


        }
        basicOutputCollector.emit(new Values(logMessage.getType(),logMessage));
        //定时更新规则信息
        LogAnalyzeHandler.scheduleLoad();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //根据点击内容类型将日志进行区分
        declarer.declare(new Fields("type", "message"));
    }
}
