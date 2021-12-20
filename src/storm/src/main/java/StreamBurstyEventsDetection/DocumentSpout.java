package StreamBurstyEventsDetection;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class DocumentSpout extends BaseRichSpout {
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
    public static final int SUBMIT_DURATION = 86400;
    public static final int SIGNAL_SUBMIT = -1;
    public static final int SIGNAL_OUTPUT = -2;
    SpoutOutputCollector _collector;
    int last_submit_time = 0;
    int document_id = 1;
    InputStream inputStream = null;
    BufferedReader bufferedReader = null;

    public static boolean isControlSignal(int id) {
        return id <= 0;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        try {
            inputStream = new FileInputStream(conf.get("news_file_path").toString());
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        if (inputStream == null)
            return;
        String s = null;
        try {
            s = bufferedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (s == null || document_id < 0) {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            inputStream = null;
            return;
        }
        String[] tokens = s.split("\t");
        int t;
        try {
            t = (int) DATE_FORMAT.parse(tokens[0]).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }
        if (t - last_submit_time >= SUBMIT_DURATION) {
            System.out.println(TIME_FORMAT.format(new Date()) + " [SPOUT] " + tokens[0] + " completed.");
            // Work as a signal
            _collector.emit(new Values(SIGNAL_SUBMIT, tokens[0]));
            last_submit_time = t;
            // NOTE: DEBUG
            document_id = -1;
            return;
        }
        _collector.emit(new Values(document_id++, tokens[1]));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "content"));
    }
}
