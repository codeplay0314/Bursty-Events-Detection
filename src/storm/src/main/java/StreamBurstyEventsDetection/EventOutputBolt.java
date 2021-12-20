package StreamBurstyEventsDetection;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class EventOutputBolt extends BaseBasicBolt {
    HashMap<String, DateFeatures> dateCollection = new HashMap<>();
    HashMap<String, WordHistory> wordCollection = new HashMap<>();

    public static class DateFeatures {
        public LinkedList<Feature> features = new LinkedList<>();
        public int ack = 0, all_count = 0;
        public static class Feature {
            public String word;
            public int count;
            public byte[] min_hash;
        }
    }

    public static class WordHistory {
        public double freq_sum = 0;
        public int freq_cnt = 0;
    }

    public void finish(String date) {
        DateFeatures dateFeatures = dateCollection.remove(date);
        System.out.println(DocumentSpout.TIME_FORMAT.format(new Date()) + " [EVENT] " + date + " completed, " +
                dateFeatures.features.size() + " features in total.");
        for (Iterator<DateFeatures.Feature> it = dateFeatures.features.iterator(); it.hasNext();) {
            DateFeatures.Feature feature = it.next();
            if (wordCollection.containsKey(feature.word)) {
                WordHistory history = wordCollection.get(feature.word);
                history.freq_sum += (double) feature.count / dateFeatures.all_count;
                history.freq_cnt++;
            } else {
                WordHistory history = new WordHistory();
                history.freq_sum = (double) feature.count / dateFeatures.all_count;
                history.freq_cnt = 1;
                wordCollection.put(feature.word, history);
                it.remove();
            }
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String date = tuple.getStringByField("date");
        if (tuple.getIntegerByField("count") == 0) {
            // ack
            if (dateCollection.containsKey(date)) {
                if (++dateCollection.get(date).ack == WordFeaturesBolt.WORD_BOLT_COUNT) {
                    finish(date);
                }
            } else {
                DateFeatures dateFeatures = new DateFeatures();
                dateFeatures.ack = 1;
                dateCollection.put(date, dateFeatures);
            }
        } else {
            // submit
            DateFeatures.Feature feature = new DateFeatures.Feature();
            feature.count = tuple.getIntegerByField("count");
            feature.word = tuple.getStringByField("word");
            feature.min_hash = tuple.getBinaryByField("min_hash");
            if (dateCollection.containsKey(date)) {
                dateCollection.get(date).features.add(feature);
            } else {
                DateFeatures dateFeatures = new DateFeatures();
                dateFeatures.all_count = feature.count;
                dateFeatures.features.add(feature);
                dateCollection.put(date, dateFeatures);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
