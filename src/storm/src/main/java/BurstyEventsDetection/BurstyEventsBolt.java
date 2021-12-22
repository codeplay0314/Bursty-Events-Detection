package BurstyEventsDetection;

import BurstyEventsDetection.lib.BurstyProb;
import BurstyEventsDetection.lib.UnionFind;
import BurstyEventsDetection.module.Event;
import BurstyEventsDetection.module.Feature;
import BurstyEventsDetection.module.FeatureInfo;
import javafx.scene.control.DateCell;
import javafx.util.Pair;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class BurstyEventsBolt extends BaseBasicBolt {

    public int cold_start_countdown = 7;
//
//    private double get_score(List<FeatureInfo> features, List<FeatureInfo> all) {
//        HashSet<String> cap = new HashSet<>(), cup = new HashSet<>();
//        for (FeatureInfo.Info i: features.get(0).get_infos()) {
//            cap.add(i.get_date());
//            cup.add(i.get_date());
//        }
//        for (int n = 1; n < features.size(); n++) {
//            for (FeatureInfo.Info i: features.get(n).get_infos()) {
//                cup.add(i.get_date());
//            }
//            HashSet<String> cap2 = new HashSet<>();
//            for (String v: cap) {
//                boolean f = false;
//                for (FeatureInfo.Info i: features.get(n).get_infos()) {
//                    if (v.equals(i.get_date())) {
//                        f = true;
//                        break;
//                    }
//                }
//                if (f)
//                    cap2.add(v);
//            }
//            cap = cap2;
//        }
//        double a = -Math.log((double) cap.size() / cup.size());
//        BitSet ad = new BitSet();
//        for (FeatureInfo f: features) {
//            BitSet d = f.get_infos()[f.get_infos().length - 1].get_doc_set();
//            ad.or(d);
//        }
//        int m = ad.cardinality();
//        for (FeatureInfo f: features) {
//            int d = f.get_infos()[f.get_infos().length - 1].get_doc_info().getKey();
//            a -= Math.log((double) d / m);
//        }
//        for (FeatureInfo f: all) {
//            if (features.contains(f))
//                break;
//            int d = f.get_infos()[f.get_infos().length - 1].get_doc_info().getKey();
//            a -= Math.log(1 - (double) d / m);
//        }
//        if (Double.isNaN(a))
//            a = Double.POSITIVE_INFINITY;
//        return a;
//    }

    class DateCompare {
        BitSet b1, b2;
    }

    boolean scoreCompare(FeatureInfo f1, FeatureInfo f2) {

        HashMap<String, DateCompare> mp = new HashMap<String, DateCompare>();
        HashSet<String> dates1 = new HashSet<String>();
        for (FeatureInfo.Info info : f1.get_infos()) {
            String date = info.get_date();
            dates1.add(date);
            DateCompare dc = mp.containsKey(date) ? mp.get(date) : new DateCompare();
            dc.b1 = (BitSet) info.get_doc_set().clone();
            dc.b2 = new BitSet(info.get_doc_info().getValue());
            mp.put(date, dc);
        }
        HashSet<String> dates2 = new HashSet<String>();
        for (FeatureInfo.Info info : f2.get_infos()) {
            String date = info.get_date();
            dates2.add(date);
            DateCompare dc = mp.containsKey(date) ? mp.get(date) : new DateCompare();
            if (dc.b1 == null) dc.b1 = new BitSet(info.get_doc_info().getValue());
            dc.b2 = (BitSet) info.get_doc_set().clone();
            mp.put(date, dc);
        }
        int a = dates1.size(), b = dates2.size();
        dates1.addAll(dates2);
        int c = dates1.size();
        double Pe1 = (double) (a + b - c) / c, Pe2 = (double) 1;

        a = b = c = 0;
        for (String date : mp.keySet()) {
            DateCompare dc = mp.get(date);
            BitSet bset = (BitSet) dc.b1.clone();
            bset.or(dc.b2);
            a += dc.b1.cardinality();
            b += dc.b2.cardinality();
            c += bset.cardinality();
        }
        double Pde1 = (double) a / c * b / c, Pde21 = (1 - (double) a / c) * b / c, Pde22 = (1 - (double) b / c) * a / c;
        double score1 = Pde1 * Pe1, score21 = Pde21 * Pe2, score22 = Pde22 * Pe2;
        return score1 > score21 && score1 > score22;
    }

    HashSet<String> word_last_day = new HashSet<String>();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String date = input.getValue(0).toString();
        List<FeatureInfo> finfos = (List<FeatureInfo>) input.getValue(1);

        List<FeatureInfo> finfo = new ArrayList<>();
        HashSet<String> word_today = new HashSet<String>();
        for (FeatureInfo f : finfos) {
//            word_today.add(f.get_feature().get());
            FeatureInfo.Info infotoday = null;
            for (FeatureInfo.Info info : f.get_infos()) {
                if (info.get_date().equals(date)) {
                    infotoday = info;
                    break;
                }
            }
            if (infotoday == null) continue;
            if (!f.isStopword() && BurstyProb.calc(infotoday.get_doc_info().getValue(), infotoday.get_doc_info().getKey(), f.get_p()) > 1e-6 &&
                    f.get_infos()[f.get_infos().length - 1].get_doc_info().getKey() >= 3) {
                finfo.add(f);
            }
        }
//        word_last_day = word_today;

        if (cold_start_countdown > 0) {
            cold_start_countdown--;
        }
        else {
            int n = finfo.size();
            UnionFind union = new UnionFind(n);

            for (int i = 0; i < n; i++) {
                for (int j = i + 1; j < n; j++) {
                    if (scoreCompare(finfo.get(i), finfo.get(j))) {
                        union.unite(i, j);
                    }
                }
            }

            List<List<Integer>> es = union.getSet();
            for (List<Integer> e : es) {
                if (e.size() > 1) {
                    List<Feature> flist = new ArrayList<Feature>();
                    for (Integer i : e) {
                        flist.add(new Feature(finfo.get(i).get_feature().get()));
                    }
                    Event event = new Event(flist);
//                    System.out.println(date + " Event Emit: " + event.list());
                    collector.emit(new Values(date, event));
                }
            }
        }
        collector.emit(new Values(date, new Event(true)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "event"));
    }
}