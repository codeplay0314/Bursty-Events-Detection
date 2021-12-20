package BurstyEventsDetection;

import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class HotPeriodBolt extends BaseRichBolt {

    OutputCollector _collector;
    int expire;

    PriorityQueue<String> dates = new PriorityQueue<String>();
    HashMap<String, HashMap<String, Pair<Integer, Integer>>> cache = new HashMap<String, HashMap<String, Pair<Integer, Integer>>>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        expire = Integer.parseInt(stormConf.get("expire_num").toString());
    }

    @Override
    public void execute(Tuple input) {
        String date = input.getValue(0).toString();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "event"));
    }

}
