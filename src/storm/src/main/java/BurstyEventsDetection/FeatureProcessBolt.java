package BurstyEventsDetection;

import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class FeatureProcessBolt extends BaseRichBolt {

    OutputCollector _collector;
    int expire;
    HashMap<String, LinkedBlockingQueue<BitSet>> dic = new HashMap<String, LinkedBlockingQueue<BitSet>>();
    HashMap<String, LinkedBlockingQueue<String>> dicdate = new HashMap<String, LinkedBlockingQueue<String>>();
    HashMap<String, LinkedBlockingQueue<Pair<Integer, Integer>>> dicdata = new HashMap<String, LinkedBlockingQueue<Pair<Integer, Integer>>>();
    HashMap<String, Pair<Integer, Integer>> p = new HashMap<String, Pair<Integer, Integer>>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        expire = Integer.parseInt(stormConf.get("expire_num").toString());
    }

    @Override
    public void execute(Tuple input) {
        String date = input.getValue(0).toString();
        String feature = input.getValue(1).toString();
        BitSet doc_set = (BitSet) input.getValue(2);
        int size = Integer.parseInt(input.getValue(3).toString());

        LinkedBlockingQueue<BitSet> history = dic.containsKey(feature)? dic.get(feature) : new LinkedBlockingQueue<BitSet>();
        LinkedBlockingQueue<String> hisdate = dicdate.containsKey(feature)? dicdate.get(feature) : new LinkedBlockingQueue<String>();
        LinkedBlockingQueue<Pair<Integer, Integer>> hisdata = dicdata.containsKey(feature)? dicdata.get(feature) : new LinkedBlockingQueue<Pair<Integer, Integer>>();
        Pair<Integer, Integer> stat = p.containsKey(feature)? p.get(feature): new Pair<Integer, Integer>(0, 0);
        if (history.size() >= expire) {
            history.poll();
            hisdate.poll();
            Pair<Integer, Integer> pair = hisdata.poll();
            stat = new Pair<Integer, Integer>(stat.getKey() - pair.getKey(), stat.getValue() - pair.getValue());
        }
        history.add(doc_set);
        hisdate.add(date);
        Pair<Integer, Integer> pair = new Pair<Integer, Integer>(doc_set.cardinality(), size);
        hisdata.add(pair);
        stat = new Pair<Integer, Integer>(stat.getKey() + pair.getKey(), stat.getValue() + pair.getValue());

        dic.put(feature, history);
        dicdate.put(feature, hisdate);
        dicdata.put(feature, hisdata);;
        p.put(feature, stat);

        _collector.emit(new Values(date, feature, dic.get(feature), dicdate.get(feature), dicdata.get(feature), p.get(feature)));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "feature", "doc_set_his", "date_set_his", "data_set_his", "feat_stat"));
    }
}
