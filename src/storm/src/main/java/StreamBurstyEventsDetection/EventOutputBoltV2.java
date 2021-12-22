package StreamBurstyEventsDetection;

import javafx.util.Pair;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class EventOutputBoltV2 extends EventOutputBolt {

    public static class DateFeatures {
        public LinkedList<DateFeatures.Feature> features = new LinkedList<>();
        public int ack = 0, all_count = 0;
        public static class Feature {
            public String word;
            public int count;
            public byte[] min_hash;
            public int history_count;
            public byte[] history_hash;
        }
    }

    HashMap<String, DateFeatures> dateCollection = new HashMap<>();
    int history_all_count = 0;

    public double test_aggregate(DateFeatures.Feature x, DateFeatures.Feature y, int n) {
        int d = MinHashCounter.compareHash(x.min_hash, y.min_hash);
        int hd = MinHashCounter.compareHash(x.history_hash, y.history_hash);
        if (hd == MinHashCounter.HASH_LENGTH)
            return d == MinHashCounter.HASH_LENGTH ? 0 : 2;
        double c = (double) (MinHashCounter.HASH_LENGTH - d) / (MinHashCounter.HASH_LENGTH * 2 - d) * (x.count + y.count);
        double hc = (double) (MinHashCounter.HASH_LENGTH - hd) / (MinHashCounter.HASH_LENGTH * 2 - hd) * (x.history_count + y.history_count);
        double p = c / n;
        double hp = hc / history_all_count;
        double o = Math.sqrt(hp * (1 - hp) / n);
//        System.out.println("[TEST] " + x.word + ", " + y.word + ": " + p + " " + hp + " " + o);
        return Math.min(2, Math.log(Math.max(0, (p - hp) / o)));
    }

    @Override
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
                        double t = test_aggregate(x, k, dateFeatures.all_count);
//                        System.out.println("[SIMILAR] " + k.word + ", " + x.word + ": " + t);
                        if (t > Math.log(BURST_THRESHOLD_SIGMA)) {
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

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String date = tuple.getStringByField("date");
        if (tuple.getIntegerByField("count") == 0) {
            // ack
            if (dateCollection.containsKey(date)) {
                DateFeatures dateFeatures = dateCollection.get(date);
                if (++dateFeatures.ack == WordFeaturesBolt.WORD_BOLT_COUNT) {
                    dateFeatures.all_count = tuple.getInteger(3);
                    history_all_count += dateFeatures.all_count;
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
            feature.history_count = tuple.getIntegerByField("history_count");
            feature.history_hash = tuple.getBinaryByField("history_hash");
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

}
