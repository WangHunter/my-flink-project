package myflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaConsumer {

//    public static void main(String[] args) {
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(1000);
//
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "kfk72.ecsmq.meiyoucloud.com:9092,kfk73.ecsmq.meiyoucloud" +
//                ".com:9092,kfk74.ecsmq.meiyoucloud.com:9092");
////        properties.put("zookeeper.connect", "kfk72.ecsmq.meiyoucloud.com:2181,kfk73.ecsmq.meiyoucloud.com:2181,kfk74.ecsmq.meiyoucloud.com:2181");
//        properties.setProperty("group.id", "flink_test");
//        properties.setProperty("offsets.topic.replication.factor", "1");
//        properties.put("auto.commit.interval.ms", "1000");
//
//        FlinkKafkaConsumer09<String> myConsumer = new FlinkKafkaConsumer09<String>("yimei_ga_log_prod", new SimpleStringSchema(), properties);
//
//        DataStream<String> stream = env.addSource(myConsumer);
//        DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new LineSplitter()).keyBy(0).sum(1);
//
//        try {
//            env.execute("WordCount from Kafka data");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }


//    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
//        private static final long serialVersionUID = 1L;
//
//        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
////            String[] tokens = value.toLowerCase().split("ξ", -1);
//            System.out.println(value+"-----------------------------");
////            for (String token : tokens) {
////                if (token.length() > 0) {
////                    out.collect(new Tuple2<String, Integer>(token, 1));
////                }
////            }
//        }
//    }

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // 每5s checkpoint一次
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kfk72.ecsmq.meiyoucloud.com:9092");
        properties.setProperty("group.id", "flink_test");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("yimei_ga_log_prod", new SimpleStringSchema(), properties);
        //默认消费策略
        consumer.setStartFromGroupOffsets();
        DataStream<String> stream = env.addSource(consumer);
        DataStream<String> stream_filter = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String line) throws Exception {
                if(line.split("Ξ", -1).length==30) {
//                    System.out.println(line);
                    return true;
                }
                else{
                    return false;
                }
            }
        });

        DataStream<String> stream_flatMap = stream_filter.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                for(String word : line.split("Ξ"))
                    collector.collect(word);
            }
        });
        stream_flatMap.print();
        env.execute();

    }
}
