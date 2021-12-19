package BurstyEventsDetection;

import javafx.beans.binding.ObjectExpression;
import javafx.util.Pair;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class DataCollectBolt extends BaseBasicBolt {

    protected class FeatureInfo {
        String feature;
        LinkedBlockingQueue<BitSet> doc_sets;
        LinkedBlockingQueue<String> dates;
        LinkedBlockingQueue<Pair<Integer, Integer>> doc_infos;
        Pair<Integer, Integer> feat_info;
    }

    HashMap<String, Integer> sizes = new HashMap<String, Integer>();
    HashMap<String, List<Object>> cache = new HashMap<String, List<Object>>();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.size() == 2) {    // from NewsSpout
            String date = input.getValue(0).toString();
            int size = Integer.parseInt(input.getValue(1).toString());

            if (!cache.containsKey(date)) cache.put(date, new ArrayList<Object>());
            List<Object> infos = cache.get(date);
            if (infos.size() >= size) {
                collector.emit(new Values(date, infos));
                infos.remove(date);
            } else {
                sizes.put(date, size);
            }
        } else {                    // from FeatureProcess
            String date = input.getValue(0).toString();
            FeatureInfo finfo = new FeatureInfo();
            finfo.feature = input.getValue(1).toString();
            finfo.doc_sets = (LinkedBlockingQueue<BitSet>) input.getValue(2);
            finfo.dates = (LinkedBlockingQueue<String>) input.getValue(3);
            finfo.doc_infos = (LinkedBlockingQueue<Pair<Integer, Integer>>) input.getValue(4);
            finfo.feat_info = (Pair<Integer, Integer>) input.getValue(5);

            if (!cache.containsKey(date)) cache.put(date, new ArrayList<Object>());
            List<Object> infos = cache.get(date);
            infos.add(finfo);
            cache.put(date, infos);

            if (!sizes.containsKey(date)) return;
            int size = sizes.get(date);
            infos = cache.get(date);
            if (infos.size() >= size) {
                collector.emit(new Values(date, infos));
                infos.remove(date);
                sizes.remove(date);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "feat_info"));
    }
}
