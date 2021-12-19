package BurstyEventsDetection;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.List;

public class BurstyEventsBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String date = input.getValue(0).toString();
        List<DataCollectBolt.FeatureInfo> finfo = (List<DataCollectBolt.FeatureInfo>) input.getValue(1);


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
