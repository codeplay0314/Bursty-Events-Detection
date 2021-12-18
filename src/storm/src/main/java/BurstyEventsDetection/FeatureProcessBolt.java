package BurstyEventsDetection;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class FeatureProcessBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String date = input.getValue(0).toString();
        String feature = input.getValue(1).toString();
        String doc_set = input.getValue(2).toString();
        System.out.println(this);
        System.out.println(date);
        System.out.println(feature);
        System.out.println(doc_set);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
