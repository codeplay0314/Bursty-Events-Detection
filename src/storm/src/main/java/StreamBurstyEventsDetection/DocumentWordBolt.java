package StreamBurstyEventsDetection;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashSet;

public class DocumentWordBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        int id = tuple.getIntegerByField("id");
        if (DocumentSpout.isControlSignal(id)) {
            // control signal
            basicOutputCollector.emit(new Values(id, tuple.getValue(1)));
        }
        else {
            // document
            String[] words = tuple.getStringByField("content").split("[^a-zA-Z]+");
            HashSet<String> wordList = new HashSet<>();
            for (String word: words) {
                if (!wordList.contains(word)) {
                    wordList.add(word);
                    basicOutputCollector.emit(new Values(id, word));
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "word"));
    }
}
