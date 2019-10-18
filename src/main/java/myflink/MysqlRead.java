package myflink;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MysqlRead {

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设定检查点
        env.enableCheckpointing(5000);
        //设定eventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);



    }
}
