package BurstyEventsDetection;

import BurstyEventsDetection.module.Feature;
import BurstyEventsDetection.module.FeatureInfo;
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
    HashMap<String, LinkedBlockingQueue<FeatureInfo.Info>> cache = new HashMap<String, LinkedBlockingQueue<FeatureInfo.Info>>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        expire = Integer.parseInt(stormConf.get("expire_num").toString());
    }

    @Override
    public void execute(Tuple input) {
        String date = input.getValue(0).toString();
        String feature = input.getValue(1).toString();
        FeatureInfo.Info info = (FeatureInfo.Info) input.getValue(2);

        LinkedBlockingQueue<FeatureInfo.Info> infos = cache.containsKey(feature)? cache.get(feature): new LinkedBlockingQueue<FeatureInfo.Info>();
        if (infos.size() >= expire) {
            infos.poll();
        }
        infos.add(info);
        cache.put(feature, infos);

        _collector.emit(new Values(date, new FeatureInfo(new Feature(feature), infos)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "feature_info"));
    }
}
