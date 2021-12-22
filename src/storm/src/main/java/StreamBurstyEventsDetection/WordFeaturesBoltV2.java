package StreamBurstyEventsDetection;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordFeaturesBoltV2 extends WordFeaturesBolt{
    HashMap<String, MinHashHistoryCounter> wordMap = new HashMap<>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        int id = tuple.getIntegerByField("id");
        String word = tuple.getStringByField("word");
        if (DocumentSpout.isControlSignal(id)) {
            // submit data
            for (Map.Entry<String, MinHashHistoryCounter> entry : wordMap.entrySet()) {
                MinHashHistoryCounter counter = entry.getValue();
                if (counter.count > MIN_COUNT_SUBMIT) {
                    // System.out.println("SUBMIT: " + entry.getKey());
                    basicOutputCollector.emit(new Values(word, entry.getKey(),
                            counter.count, counter.getMinHash(), counter.historyCount, counter.get_history()));
                }
                if (counter.count > 0)
                    counter.clear();
            }
            basicOutputCollector.emit(new Values(word, "", 0, -id, 0, 0));
        } else if (word.length() > 0) {
            if (wordMap.containsKey(word)) {
                wordMap.get(word).put(id);
            } else {
                MinHashHistoryCounter counter = new MinHashHistoryCounter();
                counter.put(id);
                wordMap.put(word, counter);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("date", "word", "count", "min_hash", "history_count", "history_hash"));
    }
}
