package BurstyEventsDetection;

import BurstyEventsDetection.module.FeatureInfo;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.lang.Math.min;

public class DataCollectBolt extends BaseBasicBolt {

    HashMap<String, Integer> sizes = new HashMap<String, Integer>();
    HashMap<String, List<Object>> cache = new HashMap<String, List<Object>>();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String SourceComponent = input.getSourceComponent();
        if (SourceComponent.equals("BurstyFeatures")) {
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
        } else if (SourceComponent.equals("FeatureProcess")) {
            String date = input.getValue(0).toString();
            FeatureInfo finfo = (FeatureInfo) input.getValue(1);

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
        declarer.declare(new Fields("date", "feat_infos"));
    }
}
