package StreamBurstyEventsDetection;

import javafx.util.Pair;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class EventOutputBolt extends BaseBasicBolt {
    public static final double BURST_THRESHOLD_SIGMA = 3;
    public static final int COLD_BOOT_DAY = 16;
    public static final double BURST_THRESHOLD_SIMILAR = 0.75;
    public static final int BURST_MIN_COUNT = 15;
    public static final int TOP_FEATURE_USE = 1000;
    public static final String OUTPUT_FILE_PATH = "/home/hadoop/event.log";
//    public static final String OUTPUT_FILE_PATH = "event.log";

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

    public boolean test_stopword(String word, int count, int all_count) {
        double n = (double) count / all_count;
        if (wordCollection.containsKey(word)) {
            WordHistory history = wordCollection.get(word);
            history.freq_sum += n;
            history.freq_cnt++;
            double p = history.freq_sum / history.freq_cnt;
//            if (p > 1 || n > 1) {
//                System.out.println("[BUG PROBABILITY] " + feature.word + ", n=" + n + ", p=" + p);
//            }
            if (p >= 0.8 || n >= 0.8 || count < BURST_MIN_COUNT)
                return true;
            else {
                double o = Math.sqrt(p * (1 - p) / all_count);
                double d = (n - p) / o;
                return d < BURST_THRESHOLD_SIGMA;
            }
        } else {
            WordHistory history = new WordHistory();
            history.freq_sum = (double) count / all_count;
            history.freq_cnt = 1;
            wordCollection.put(word, history);
            return true;
        }
    }

    public double test_aggregate(DateFeatures.Feature x, DateFeatures.Feature y) {
        int d = MinHashCounter.compareHash(x.min_hash, y.min_hash);
//        double t = (double) (128 - d) / (256 - d) * (x.count + y.count) / Math.sqrt(x.count * y.count);
        return (double) (MinHashCounter.HASH_LENGTH - d) / (MinHashCounter.HASH_LENGTH * 2 - d)
                * (x.count + y.count) / Math.min(x.count, y.count);
    }

    public void finish(String date) {
        DateFeatures dateFeatures = dateCollection.remove(date);
        System.out.println(DocumentSpout.TIME_FORMAT.format(new Date()) + " [EVENT] " + date + " completed, " +
                dateFeatures.features.size() + " features in total.");
        if (++finished_cnt <= COLD_BOOT_DAY)
            return;

        dateFeatures.features.removeIf(feature -> test_stopword(feature.word, feature.count, dateFeatures.all_count));
        System.out.println("There are " + wordCollection.size() + " word history records.");
        System.out.println("There are " + dateFeatures.features.size() + " bursty features.");
        dateFeatures.features.sort((o1, o2) -> -Integer.compare(o1.count, o2.count));
        List<DateFeatures.Feature> features = dateFeatures.features.subList(0,
                Math.min(TOP_FEATURE_USE, dateFeatures.features.size()));

//        for (DateFeatures.Feature feature: dateFeatures.features) {
//            System.out.println("Feature " + feature.word + ", count=" + feature.count);
//        }

        ArrayList<Pair<Pair<Integer, Double>, List<String>>> events = new ArrayList<>();
        boolean first = true;
        while (features.size() > 0) {
            double score_sum = 0;
            int score_cnt = 0;
            ArrayList<DateFeatures.Feature> feat = new ArrayList<>();
            for (Iterator<DateFeatures.Feature> it = features.iterator(); it.hasNext();) {
                DateFeatures.Feature x = it.next();
                if (feat.size() == 0) {
                    feat.add(x);
                    it.remove();
                } else {
                    double score_part_sum = 0;
                    boolean add_feature = true;
                    for (DateFeatures.Feature k: feat) {
                        double t = test_aggregate(x, k);
//                        System.out.println("[SIMILAR] " + k.word + ", " + x.word + ": " + t);
                        if (t > BURST_THRESHOLD_SIMILAR) {
                            score_part_sum += t;
                        } else {
                            add_feature = false;
                            break;
                        }
                    }
                    if (add_feature) {
                        score_sum += score_part_sum;
                        score_cnt += feat.size();
                        feat.add(x);
                        it.remove();
                    }
                }
            }
            ArrayList<String> words = new ArrayList<>();
            int min_count = Integer.MAX_VALUE;
            for (DateFeatures.Feature f: feat) {
                min_count = Integer.min(min_count, f.count);
                words.add(f.word);
            }
            if (score_cnt > 0) {
                events.add(new Pair<>(new Pair<>(min_count, score_sum / score_cnt), words));
//                System.out.println("[EVENT] score=" + (score_sum / score_cnt));
//                for (DateFeatures.Feature f: feat)
//                    System.out.println("[EVENT FEATURE] " + f.word + ", count=" + f.count + ", hash=" + Arrays.toString(f.min_hash));
            } else if (first && feat.size() > 0) {
                int c2 = features.size() > 0 ? features.get(0).count : 1;
                double r = (double) feat.get(0).count / c2 - 1;
                if (r > BURST_THRESHOLD_SIMILAR)
                    events.add(new Pair<>(new Pair<>(min_count, r), words));
            }
            first = false;
        }

        System.out.println("There are " + events.size() + " bursty events.");
        output_events(date, events);
    }

    public void output_events(String date, List<Pair<Pair<Integer, Double>, List<String>>> events) {
        events.sort((o1, o2) -> -Double.compare(o1.getKey().getKey(), o2.getKey().getKey()));
        try {
            if (bufferedWriter == null) {
                bufferedWriter = new BufferedWriter(new FileWriter(OUTPUT_FILE_PATH));
            }
            bufferedWriter.write(date + "\n");
            for (Pair<Pair<Integer, Double>, List<String>> event: events) {
                System.out.println("Event " + event.getValue().toString() +
                        ", count=" + event.getKey().getKey() + ", score=" + event.getKey().getValue());
                bufferedWriter.write(event.getValue().toString() + " " +
                        event.getKey().getKey() + " " + event.getKey().getValue() + "\n");
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
