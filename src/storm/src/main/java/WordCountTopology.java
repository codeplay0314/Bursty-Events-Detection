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

public class WordCountTopology {

    /**
     * Spout - 继承基础的BaseRichSpout，尝试去数据源获取数据，比如说从kafka中消费数据
     */
    public static class RandomSentenceSpout extends BaseRichSpout {

        SpoutOutputCollector _collector;
        Random _rand;

        /**
         * open方法是对Spout 进行初始化的
         * 比如创建线程池、数据库连接池、构造一个httpclient
         * 在open方法初始化的时候，会传入一个SpoutOutputCollector，这个类就是用来发射数据的。
         */
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;
            //构造一个随机生产对象
            _rand = new Random();
        }


        /**
         * 这个Spout之前说过，最终会运行在task中，某个worker进程的某个excuter执行内部的某个task
         * 这个task会无限循环的去调用nextTuple()方法
         * 这就可以不断发射最新的数据出去，形成一个数据流Stream
         */
        public void nextTuple() {
//            String[] sentences = new String[]{sentence("the cow jumped over the moon"), sentence("an apple a day keeps the doctor away"),
//                    sentence("four score and seven years ago"), sentence("snow white and the seven dwarfs"), sentence("i am at two with nature")};
//            // 从sentences.length的长度中随机获取一个整数
//            final String sentence = sentences[_rand.nextInt(sentences.length)];
//            // 发射数据
            FileInputStream inputStream = null;
            try {
                inputStream = new FileInputStream("data/news.txt");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String str = null;
            String date = null;
            int cnt = 0;
            while (true) {
                try {
                    str = bufferedReader.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (str == null) break;
                String[] tokens = str.split("\t");
                if (date != tokens[0]) {
                    Utils.sleep(100);
                    date = tokens[0];
                    System.out.println("date: " + date);
                    if (++cnt >= 2) break;
                }
                String sentence = tokens[1];
                System.out.println("RandomSentenceSpout emitting: " + sentence);
                _collector.emit(new Values(sentence));
            }

            //close
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

        protected String sentence(String input) {
            return input;
        }

        @Override
        public void ack(Object id) {
        }

        @Override
        public void fail(Object id) {
        }

        /**
         * declareOutputFields方法：定义一个你发出去的每个tuple中的每个field的名称
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }

    }

    /**
     * bolt ： spout将数据传给bolt，
     * 每个bolt同样是发送到某个worker进程的某个excuter执行内部的某个task
     */
    public static class SplitSentence implements IRichBolt {

        private static final long serialVersionUID = 6604009953652729483L;

        private OutputCollector collector;

        /**
         * 对于bolt来说，第一个方法，就是prepare方法，初始化的方法
         * 传入的OutputCollector，这个也是Bolt的这个tuple的发射器
         */
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        /**
         * execute方法
         * 就是说，每次接收到一条数据后，就会交给这个executor方法来执行
         */
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] words = sentence.split(" ");
            System.out.println("SplitSentence splitting: " + sentence);
            for (String word : words) {
                System.out.println("SplitSentence emitting: " + word);
                collector.emit(new Values(word));
            }
        }

        public void cleanup() {

        }

        /**
         * 定义发射出去的tuple，每个field的名称
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }


    public static class WordCount extends BaseRichBolt {

        private static final long serialVersionUID = 7208077706057284643L;

        private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

        private OutputCollector collector;
        private final Map<String, Long> wordCounts = new HashMap<String, Long>();

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");

            Long count = wordCounts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;

            wordCounts.put(word, count);

            LOGGER.info(word + " has appeared " + count + " times till now");

            collector.emit(new Values(word, count));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

    }


    public static void main(String[] args) throws Exception {
        // 在main方法中，会去将spout和bolts组合起来，构建成一个拓扑
        TopologyBuilder builder = new TopologyBuilder();

        // 这里的第一个参数的意思，就是给这个spout设置一个名字
        // 第二个参数的意思，就是创建一个spout的对象
        // 第三个参数的意思，就是设置spout的executor有几个
        builder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);
        builder.setBolt("SplitSentence", new SplitSentence(), 5)
                .setNumTasks(10)
                .shuffleGrouping("RandomSentence");
        // 这个很重要，就是说，相同的单词，从SplitSentence发射出来时，一定会进入到下游的指定的同一个task中
        // 只有这样子，才能准确的统计出每个单词的数量
        // 比如你有个单词，hello，下游task1接收到3个hello，task2接收到2个hello
        // 所以这5个hello，应该全都进入一个task，这样出来的就是5个hello。
        builder.setBolt("WordCount", new WordCount(), 10)
                .setNumTasks(20)
                .fieldsGrouping("SplitSentence", new Fields("word"));

        Config config = new Config();

        // 说明是在命令行执行，打算提交到storm集群上去
        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // 说明是在eclipse里面本地运行

            config.setMaxTaskParallelism(20);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WordCountTopology", config, builder.createTopology());

            Utils.sleep(60000);

            cluster.shutdown();
        }
    }
}