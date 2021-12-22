package StreamBurstyEventsDetection;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class WordFeaturesBolt extends BaseBasicBolt {
    public static final int MIN_COUNT_SUBMIT = 1;
    public static final int WORD_BOLT_COUNT = 32;
    HashMap<String, MinHashCounter> wordMap = new HashMap<>();

    public static class WordStreamGrouping implements CustomStreamGrouping {
        List<Integer> targetTasks;
        int numTasks;

        @Override
        public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
            targetTasks = list;
            numTasks = targetTasks.size();
        }

        @Override
        public List<Integer> chooseTasks(int i, List<Object> list) {
            int id = (int) list.get(0);
            if (DocumentSpout.isControlSignal(id))
                return targetTasks; // all grouping
            int t = Math.abs(list.get(1).toString().hashCode()) % numTasks;
            return Collections.singletonList(targetTasks.get(t));
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        int id = tuple.getIntegerByField("id");
        String word = tuple.getStringByField("word");
        if (DocumentSpout.isControlSignal(id)) {
            // submit data
            for (Map.Entry<String, MinHashCounter> entry : wordMap.entrySet()) {
                MinHashCounter counter = entry.getValue();
                if (counter.count > MIN_COUNT_SUBMIT) {
                    // System.out.println("SUBMIT: " + entry.getKey());
                    basicOutputCollector.emit(new Values(word, entry.getKey(), counter.count, counter.getMinHash()));
                }
                if (counter.count > 0)
                    counter.clear();
            }
            basicOutputCollector.emit(new Values(word, "", 0, -id));
        } else if (word.length() > 0) {
            if (wordMap.containsKey(word)) {
                wordMap.get(word).put(id);
            } else {
                MinHashCounter counter = new MinHashCounter();
                counter.put(id);
                wordMap.put(word, counter);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("date", "word", "count", "min_hash"));
    }
}
