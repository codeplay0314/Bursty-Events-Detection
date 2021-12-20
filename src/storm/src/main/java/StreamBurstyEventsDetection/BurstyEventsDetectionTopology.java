package StreamBurstyEventsDetection;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class BurstyEventsDetectionTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("documents", new DocumentSpout(), 1);
        builder.setBolt("words", new DocumentWordBolt(), 3)
                .shuffleGrouping("documents");
        builder.setBolt("features", new WordFeaturesBolt(), WordFeaturesBolt.WORD_BOLT_COUNT)
                .customGrouping("words", new WordFeaturesBolt.WordStreamGrouping());
        builder.setBolt("events", new EventOutputBolt(), 1)
                .globalGrouping("features");

        Config conf = new Config();
        conf.put("news_file_path", "F:\\projects\\Bursty-Events-Detection\\data\\news.txt");
        if (args != null && args.length > 0) {
            // storm
            conf.setNumWorkers(2);
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // local
            conf.setMaxTaskParallelism(16);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("BurstyEventsDetectionTopology", conf, builder.createTopology());
            Utils.sleep(10 * 1000);
            cluster.shutdown();
        }
    }
}
