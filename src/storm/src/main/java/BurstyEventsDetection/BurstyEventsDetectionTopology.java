package BurstyEventsDetection;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class BurstyEventsDetectionTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("News", new NewsSpout(), 1);
        builder.setBolt("BurstyFeatures", new BurstyFeaturesBolt(), 5).shuffleGrouping("News");
        builder.setBolt("FeatureProcess", new FeatureProcessBolt(), 10)
                .fieldsGrouping("BurstyFeatures", new Fields("date", "feature"));

        Config conf = new Config();
        conf.put("interval", 3000);
        conf.put("news_file_path", "C:/Users/codep/Desktop/Bursty-Events-Detection/data/news");

        if (args != null && args.length > 0) {
            // storm
            conf.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // local
            conf.setMaxTaskParallelism(20);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("BurstyEventsDetectionTopology", conf, builder.createTopology());

            Utils.sleep(60000);

            cluster.shutdown();
        }
    }
}