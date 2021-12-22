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
    public static final int SUBMIT_WAIT_MS = 600;

    SpoutOutputCollector _collector;
    int last_submit_time = 0;
    int submit_start_id = 1;
    int document_id = 1;
    String last_submit_date = null;
    InputStream inputStream = null;
    BufferedReader bufferedReader = null;

    public static boolean isControlSignal(int id) {
        return id <= 0;
    }

    private void submit() {
        try {
            Thread.sleep(SUBMIT_WAIT_MS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(TIME_FORMAT.format(new Date()) + " [SPOUT] " + last_submit_date + " completed.");
        // Work as a signal
        _collector.emit(new Values(submit_start_id - document_id, last_submit_date));
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

        if (s == null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            inputStream = null;
            if (last_submit_date != null)
                submit();
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
        if (last_submit_time == 0) {
            last_submit_time = t;
            last_submit_date = tokens[0];
        }

        if (t - last_submit_time >= SUBMIT_DURATION) {
            submit();
            last_submit_time = t;
            submit_start_id = document_id;
            last_submit_date = tokens[0];
        }

        _collector.emit(new Values(document_id++, tokens[1]));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "content"));
    }
}
