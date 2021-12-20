package BurstyEventsDetection;

import BurstyEventsDetection.module.Event;
import BurstyEventsDetection.module.FeatureInfo;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
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
    HashMap<String, HashMap<String, Double>> cache = new HashMap<String, HashMap<String, Double>>();
    HashMap<String, Double> P;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        expire = Integer.parseInt(stormConf.get("expire_num").toString());
    }

    @Override
    public void execute(Tuple input) {
        String SourceComponent = input.getSourceComponent();
        if (SourceComponent.equals("BurstyEvents")) {
            String date = input.getValue(0).toString();
            Event event = (Event) input.getValue(1);
            if (event == null) {
                cache.remove(date);
            } else {

            }
        } else if (SourceComponent.equals("DataCollect")) {
            String date = input.getValue(0).toString();
            List<FeatureInfo> finfo = (List<FeatureInfo>) input.getValue(1);

            HashMap<String, Pair<Integer, Integer>> rec = new HashMap<String, Pair<Integer, Integer>>();
            for (FeatureInfo info : finfo) {

            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "event"));
    }

}
