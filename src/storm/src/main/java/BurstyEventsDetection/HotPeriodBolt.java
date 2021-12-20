package BurstyEventsDetection;

import BurstyEventsDetection.lib.BurstyProb;
import BurstyEventsDetection.lib.Calc;
import BurstyEventsDetection.module.Event;
import BurstyEventsDetection.module.Feature;
import BurstyEventsDetection.module.FeatureInfo;
import javafx.util.Pair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.*;

import static java.lang.Math.max;

public class HotPeriodBolt extends BaseRichBolt {

    OutputCollector _collector;
    int expire;

    HashMap<String, List<Event>> event_list = new HashMap<String, List<Event>>();
    HashMap<String, HashMap<String, Pair<Integer, Integer>>> cache = new HashMap<String, HashMap<String, Pair<Integer, Integer>>>();
    HashMap<String, Double> P = new HashMap<String, Double>();

    private void isBurstEvent(Event e, String date) {
        double p = 0;
        List<Double> plist = new ArrayList<Double>();
        for (String day : cache.keySet()) {
            double avgp = 0;
            int cnt = 0;
            HashMap<String, Pair<Integer, Integer>> Info = cache.get(day);
            for (Object f : e.get()) {
                String feature = ((Feature) f).get();
                if (Info.containsKey(feature)) {
                    Pair<Integer, Integer> pair = Info.get(feature);
                    avgp += BurstyProb.calc(pair.getValue(), pair.getKey(), P.get(feature));
                    cnt++;
                }
            }
            plist.add(avgp / cnt);
            if (date.equals(day)) p = avgp / cnt;
        }
        Double[] ps = plist.toArray(new Double[0]);
        if (p > Calc.avg(ps) + 2 * Calc.dev(ps)) {
            System.out.println("Bursty Events: " + e.list() + " on " + date);
        } else {
            System.out.println("trump is not bursty on " + date);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        expire = Integer.parseInt(stormConf.get("expire_num").toString());
    }

    @Override
    public void execute(Tuple input) {
        String SourceComponent = input.getSourceComponent();
        if (SourceComponent.equals("BurstyEvents")) {
            String date = input.getValue(0).toString();
            Event e = (Event) input.getValue(1);
            if (e.ends()) {
                event_list.remove(date);
            } else {
                if (cache.containsKey(date)) {
                    isBurstEvent(e, date);
                } else {
                    List<Event> elist = event_list.containsKey(date)? event_list.get(date): new ArrayList<Event>();
                    elist.add(e);
                    event_list.put(date, elist);
                }
            }
        } else if (SourceComponent.equals("DataCollect")) {
            String date = input.getValue(0).toString();
            List<FeatureInfo> finfo = (List<FeatureInfo>) input.getValue(1);

            HashMap<String, Pair<Integer, Integer>> rec = new HashMap<String, Pair<Integer, Integer>>();
            for (FeatureInfo info : finfo) {
                P.put(info.get_feature().get(), info.get_p());
                for (FeatureInfo.Info i : info.get_infos()) {
                    if (i.get_date().equals(date)) {
                        rec.put(info.get_feature().get(), i.get_doc_info());
                    }
                }
            }
            cache.put(date, rec);

            if (cache.size() >= expire) {
                String[] dates = cache.keySet().toArray(new String[0]);
                Arrays.sort(dates);
                int r = dates.length, l = max(0, r - expire);
                Arrays.copyOfRange(dates, l, r);
                for (String d : dates) {
                    event_list.remove(d);
                    cache.remove(d);
                }
            }
            if (event_list.containsKey(date)) {
                List<Event> events = event_list.remove(date);
                for (Event e : events) {
                    if (e.ends()) {
                        cache.remove(date);
                    }
                    else {
                        isBurstEvent(e, date);
                    }
                }
            }
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "event"));
    }

}
