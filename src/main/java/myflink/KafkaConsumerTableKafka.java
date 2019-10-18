package myflink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class KafkaConsumerTableKafka {
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kfk72.ecsmq.meiyoucloud.com:9092");
//        properties.put("zookeeper.connect", "kfk72.ecsmq.meiyoucloud.com:2181,kfk73.ecsmq.meiyoucloud.com:2181,kfk74.ecsmq.meiyoucloud.com:2181");
        properties.setProperty("group.id", "flink_test");

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, fsSettings);

        /*tableEnv.connect(new Kafka().topic("yimei_ga_log_prod").properties(properties).startFromGroupOffsets())
                                .withFormat(new Csv()
                                .field("ip", Types.STRING)
                                .field("access_time", Types.STRING)
                                .fieldDelimiter("Ξ")
                ).withSchema(new Schema().field("ip", Types.STRING)
                .field("access_time", Types.STRING))
                .inAppendMode()
                .registerTableSource("beauty_ga_access");*/

        Table table = tableEnv.sqlQuery("select  *  from beauty_ga_access   ");
        tableEnv.toAppendStream(table, Row.class).print();
        tableEnv.execute("demo");
    }
}
