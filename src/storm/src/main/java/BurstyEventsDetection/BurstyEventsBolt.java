package BurstyEventsDetection;

import BurstyEventsDetection.module.Event;
import BurstyEventsDetection.module.Feature;
import BurstyEventsDetection.module.FeatureInfo;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

public class BurstyEventsBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String date = input.getValue(0).toString();
        List<FeatureInfo> finfo = (List<FeatureInfo>) input.getValue(1);

        // Todo: emit events
        // Event[] events;
        // for (Event e : events) {
        //     collector.emit(new Values(date, e));
        // }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "event"));
    }
}
