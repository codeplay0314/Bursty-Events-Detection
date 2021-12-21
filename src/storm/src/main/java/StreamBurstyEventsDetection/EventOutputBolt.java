package StreamBurstyEventsDetection;

import javafx.util.Pair;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.trident.operation.builtin.Min;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class EventOutputBolt extends BaseBasicBolt {
    public static final double BURST_THRESHOLD_SIGMA = 2;
    public static final int COLD_BOOT_DAY = 14;
    public static final double BURST_THRESHOLD_SIMILAR = 1 / Math.sqrt(2);
    public static final int BURST_MIN_COUNT = 5;

    HashMap<String, DateFeatures> dateCollection = new HashMap<>();
    HashMap<String, WordHistory> wordCollection = new HashMap<>();
    int finished_cnt = 0;
    BufferedWriter bufferedWriter = null;

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
        if (++finished_cnt <= COLD_BOOT_DAY)
            return;

        for (Iterator<DateFeatures.Feature> it = dateFeatures.features.iterator(); it.hasNext();) {
            DateFeatures.Feature feature = it.next();
            if (wordCollection.containsKey(feature.word)) {
                WordHistory history = wordCollection.get(feature.word);
                double n = (double) feature.count / dateFeatures.all_count;
                history.freq_sum += n;
                history.freq_cnt++;
                double p = history.freq_sum / history.freq_cnt;
//                if (p > 1 || n > 1) {
//                    System.out.println("[BUG PROBABILITY] " + feature.word + ", n=" + n + ", p=" + p);
//                }
                if (p >= 0.8 || n >= 0.8 || feature.count < BURST_MIN_COUNT)
                    it.remove();
                else {
                    double o = Math.sqrt(p * (1 - p) / dateFeatures.all_count);
                    double d = (n - p) / o;
                    if (d < BURST_THRESHOLD_SIGMA)
                        it.remove();
//                    else {
//                        // Bursty feature found
//                        System.out.println("[DISTRIBUTION] " + n + ", " + p + ", " + dateFeatures.all_count + ", " + o + ", " + d);
//                        System.out.println("[FEATURE] " + date + " Bursty feature: " + feature.word + ", sigma=" + d);
//                    }
                }
            } else {
                WordHistory history = new WordHistory();
                history.freq_sum = (double) feature.count / dateFeatures.all_count;
                history.freq_cnt = 1;
                wordCollection.put(feature.word, history);
                it.remove();
            }
        }
        System.out.println("There are " + wordCollection.size() + " word history records.");
        dateFeatures.features.sort((o1, o2) -> -Integer.compare(o1.count, o2.count));
        System.out.println("There are " + dateFeatures.features.size() + " bursty features.");

//        for (DateFeatures.Feature feature: dateFeatures.features) {
//            System.out.println("Feature " + feature.word + ", count=" + feature.count);
//        }

        ArrayList<Pair<Double, List<String>>> events = new ArrayList<>();
        boolean first = true;
        while (dateFeatures.features.size() > 0) {
            double score_sum = 0;
            int score_cnt = 0;
            ArrayList<String> feat = new ArrayList<>();
            DateFeatures.Feature k = null;
            for (Iterator<DateFeatures.Feature> it = dateFeatures.features.iterator(); it.hasNext();) {
                DateFeatures.Feature x = it.next();
                if (k == null) {
                    k = x;
                    feat.add(k.word);
                    it.remove();
                } else {
                    int d = MinHashCounter.compareHash(k.min_hash, x.min_hash);
                    double t = (double) (128 - d) / (256 - d) * (k.count + x.count) / Math.sqrt(k.count * x.count);
//                    System.out.println("[SIMILAR] " + k.word + ", " + x.word + ": " + t);
                    if (t > BURST_THRESHOLD_SIMILAR) {
                        score_sum += t;
                        score_cnt++;
                        feat.add(x.word);
//                        k = x;
                        it.remove();
                    }
                }
            }
            if (score_cnt > 0) {
                events.add(new Pair<>(score_sum / score_cnt, feat));
            } else if (first && k != null) {
                int c2 = dateFeatures.features.size() > 0 ? dateFeatures.features.getFirst().count : 1;
                double r = (double) k.count / c2 - 1;
                if (r > BURST_THRESHOLD_SIMILAR)
                    events.add(new Pair<>(r, feat));
            }
            first = false;
        }
        events.sort((o1, o2) -> -Double.compare(o1.getKey(), o2.getKey()));
        System.out.println("There are " + events.size() + " bursty events.");

        try {
            if (bufferedWriter == null) {
                bufferedWriter = new BufferedWriter(new FileWriter("event.log"));
            }
            bufferedWriter.write(date + "\n");
            for (Pair<Double, List<String>> event: events) {
                System.out.println("Event " + event.getValue().toString() + ", score=" + event.getKey());
                bufferedWriter.write(event.getValue().toString() + " " + event.getKey() + "\n");
            }
            bufferedWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String date = tuple.getStringByField("date");
        if (tuple.getIntegerByField("count") == 0) {
            // ack
            if (dateCollection.containsKey(date)) {
                DateFeatures dateFeatures = dateCollection.get(date);
                if (++dateFeatures.ack == WordFeaturesBolt.WORD_BOLT_COUNT) {
                    dateFeatures.all_count = tuple.getInteger(3);
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
                DateFeatures dateFeatures = dateCollection.get(date);
                dateFeatures.features.add(feature);
            } else {
                DateFeatures dateFeatures = new DateFeatures();
                dateFeatures.features.add(feature);
                dateCollection.put(date, dateFeatures);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
