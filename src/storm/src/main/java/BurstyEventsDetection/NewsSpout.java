package BurstyEventsDetection;

import BurstyEventsDetection.module.Document;
import BurstyEventsDetection.module.Feature;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class NewsSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    int Interval;
    String file_path;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        Interval = Integer.parseInt(conf.get("interval").toString());
        file_path = conf.get("input_file_path").toString();
    }

    @Override
    public void nextTuple() {
        FileInputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file_path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        assert inputStream != null;
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String cur_date = "";
        List<Object> docs = new ArrayList<Object>();
        while (true) {
            String str = null;
            try {
                str = bufferedReader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (str == null) break;
            String[] tokens = str.split("\t");
            if (!cur_date.equals(tokens[0])) {
                if (!cur_date.equals("")) {
                    System.out.println(cur_date + " starts at " + new Date());
                    _collector.emit(new Values(cur_date, docs));
                    docs = new ArrayList<Object>();
                    Utils.sleep(Interval);
                }
                cur_date = tokens[0];
            }
            String[] features = tokens[1].split(" ");
            Document doc = new Document();
            for (String feat : features) {
                if (feat.length() > 1) {
                    doc.add(new Feature(feat));
                }
            }
            docs.add(doc);
        }
        if (docs.size() > 0) {
            System.out.println(cur_date + " starts at " + new Date());
            _collector.emit(new Values(cur_date, docs));
        }

        try {
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "documents"));
    }
}
