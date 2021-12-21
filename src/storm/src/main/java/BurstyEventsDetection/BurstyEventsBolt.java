package BurstyEventsDetection;

import BurstyEventsDetection.module.Event;
import BurstyEventsDetection.module.Feature;
import BurstyEventsDetection.module.FeatureInfo;
import javafx.util.Pair;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.lang.reflect.Array;
import java.util.*;

public class BurstyEventsBolt extends BaseBasicBolt {

    public int cold_start_countdown = 7;

    private double get_score(List<FeatureInfo> features, List<FeatureInfo> all) {
        HashSet<String> cap = new HashSet<>(), cup = new HashSet<>();
        for (FeatureInfo.Info i: features.get(0).get_infos()) {
            cap.add(i.get_date());
            cup.add(i.get_date());
        }
        for (int n = 1; n < features.size(); n++) {
            for (FeatureInfo.Info i: features.get(n).get_infos()) {
                cup.add(i.get_date());
            }
            HashSet<String> cap2 = new HashSet<>();
            for (String v: cap) {
                boolean f = false;
                for (FeatureInfo.Info i: features.get(n).get_infos()) {
                    if (v.equals(i.get_date())) {
                        f = true;
                        break;
                    }
                }
                if (f)
                    cap2.add(v);
            }
            cap = cap2;
        }
        double a = -Math.log((double) cap.size() / cup.size());
        BitSet ad = new BitSet();
        for (FeatureInfo f: features) {
            BitSet d = f.get_infos()[f.get_infos().length - 1].get_doc_set();
            ad.or(d);
        }
        int m = ad.cardinality();
        for (FeatureInfo f: features) {
            int d = f.get_infos()[f.get_infos().length - 1].get_doc_info().getKey();
            a -= Math.log((double) d / m);
        }
        for (FeatureInfo f: all) {
            if (features.contains(f))
                break;
            int d = f.get_infos()[f.get_infos().length - 1].get_doc_info().getKey();
            a -= Math.log(1 - (double) d / m);
        }
        if (Double.isNaN(a))
            a = Double.POSITIVE_INFINITY;
        return a;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String date = input.getValue(0).toString();
        List<FeatureInfo> finfo = (List<FeatureInfo>) input.getValue(1);

        // Todo: This algorithm is n^3, thus the number of features should < 10^3
        ArrayList<Pair<List<Double>, ArrayList<FeatureInfo>>> events = new ArrayList<>();
        ArrayList<FeatureInfo> feat = new ArrayList<>();
         for (FeatureInfo f: finfo)
             if (!f.isStopword())
                 feat.add(f);
         feat.sort((f1, f2) -> -Integer.compare(
                 f1.get_infos()[f1.get_infos().length - 1].get_doc_info().getKey(),
                 f2.get_infos()[f2.get_infos().length - 1].get_doc_info().getKey()));

        if (cold_start_countdown > 0) {
            cold_start_countdown--;
            System.out.println("COLD START COUNTDOWN: " + cold_start_countdown);
        }
        else {
            while (feat.size() > 0) {
                FeatureInfo k = feat.remove(0);
                double min_cost = Double.POSITIVE_INFINITY;
                Pair<List<Double>, ArrayList<FeatureInfo>> min_event = null;
                FeatureInfo min_feat = null;
                for (Pair<List<Double>, ArrayList<FeatureInfo>> e: events) {
                    e.getValue().add(k);
                    double c = get_score(e.getValue(), feat);
                    if (c < min_cost) {
                        min_cost = c;
                        min_event = e;
                    }
                    e.getValue().remove(k);
                }
                for (FeatureInfo f: feat) {
                    ArrayList<FeatureInfo> e = new ArrayList<>();
                    e.add(k);
                    e.add(f);
                    double c = get_score(e, feat);
                    if (c < min_cost) {
                        ArrayList<Double> cc = new ArrayList<>();
                        cc.add(c);
                        min_cost = c;
                        min_event = new Pair<>(cc, e);
                        min_feat = f;
                    }
                }
                if (min_feat != null) {
                    feat.remove(min_feat);
                    events.add(min_event);
                } else if (min_event != null) {
                    if (min_cost < min_event.getKey().get(0)) {
                        min_event.getKey().set(0, min_cost);
                        min_event.getValue().add(k);
                    }
                }
            }
        }

        events.sort(Comparator.comparingDouble(e -> e.getKey().get(0)));
        for (Pair<List<Double>, ArrayList<FeatureInfo>> e: events) {
            System.out.println("EVENT " + e.getValue().toString() + ", cost=" + e.getKey().get(0));
        }
        // TODO: emit events
//         for (Event e : events) {
//             collector.emit(new Values(date, e));
//         }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "event"));
    }
}
