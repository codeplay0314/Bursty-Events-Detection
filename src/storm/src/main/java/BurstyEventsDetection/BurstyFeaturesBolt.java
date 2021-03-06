package BurstyEventsDetection;

import BurstyEventsDetection.module.Document;
import BurstyEventsDetection.module.Feature;
import BurstyEventsDetection.module.FeatureInfo;
import javafx.util.Pair;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

public class BurstyFeaturesBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String date = input.getValue(0).toString();
        List<Document> docs = (List<Document>) input.getValue(1);

        int no = 0, len = docs.size();
        HashMap<String, BitSet> index = new HashMap<String, BitSet>();
        for (Document doc : docs) {
            for (Feature feat : doc.get()) {
                String key = feat.get();
                BitSet bset = index.containsKey(key)? index.get(key): new BitSet(len);
                bset.set(no);
                index.put(key, bset);
            }
            no++;
        }

        collector.emit("FeatureCount", new Values(date, index.size()));
        for (String key : index.keySet()) {
            BitSet bset = index.get(key);
            collector.emit(new Values(date, key, new FeatureInfo.Info(date, bset, new Pair<Integer, Integer>(bset.cardinality(), len))));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "feature", "featinfo_info"));
        declarer.declareStream("FeatureCount", new Fields("date", "count"));
    }
}
