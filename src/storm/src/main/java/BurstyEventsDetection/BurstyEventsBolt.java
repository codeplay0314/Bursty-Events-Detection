package BurstyEventsDetection;

import BurstyEventsDetection.module.FeatureInfo;
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
        List<FeatureInfo> finfo = (List<FeatureInfo>) input.getValue(1);

        System.out.println("------ BurstyEventsBolt -----");
        System.out.println(date);
        for (int i = 0; i < 5; i++) {
            FeatureInfo f = finfo.get(i);
            FeatureInfo.Info info = f.get_infos()[0];
            System.out.printf("%s: %s %d %d\n", f.get_feature().get(),info.get_date(), info.get_doc_info().getKey(), info.get_doc_info().getValue());
        }


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
