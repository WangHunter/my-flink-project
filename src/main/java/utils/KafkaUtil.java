package utils;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by 11916 on 2017/8/11.
 */
public class KafkaUtil {
    private static ConsumerConnector consumer=null;

    /**
     *启动一个消费程序
     * @param topic 要消费的topic名称
     * @param handler 自己的处理逻辑的实现
     * @param threadCount 消费线程数，该值应小于等于partition个数，多了也没用
     */
    public static <T extends Serializable>void startConsumer(String topic,String groupId,final MqMessageHandler<String> handler,int threadCount) throws Exception{
        //设置处理消息线程数，线程数应小于等于partition数量，若线程数大于partition数量，则多余的线程则闲置，不会进行工作
        if(threadCount<1)
            throw new Exception("处理消息线程数最少为1");

        //消费者配置文件
        Properties props = new Properties();
        //zookeeper地址
        props.put("zookeeper.connect", "kfk72.ecsmq.meiyoucloud.com:2181,kfk73.ecsmq.meiyoucloud.com:2181,kfk74.ecsmq.meiyoucloud.com:2181");
        //组id
        props.put("group.id", groupId);
        //自动提交消费情况间隔时间
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConfig consumerConfig=new ConsumerConfig(props);
        consumer= Consumer.createJavaConsumerConnector(consumerConfig);

        //key:topic名称 value:线程数
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(threadCount));
        StringDecoder keyDecoder=new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder=new StringDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        //声明一个线程池，用于消费各个partition
        ExecutorService executor=Executors.newFixedThreadPool(threadCount);
        //获取对应topic的消息队列
        List<KafkaStream<String, String>> streams = consumerMap.get(topic);
        //为每一个partition分配一个线程去消费
        for (final KafkaStream stream : streams) {
            executor.execute(new Runnable() {
//                @Override
                public void run() {
                    ConsumerIterator<String, String> it = stream.iterator();
                    //有信息则消费，无信息将会阻塞
                    while (it.hasNext()){
                        String message=null;
                        try {
                            //将字节码反序列化成相应的对象
                            message = it.next().message();
                        } catch (Exception e) {
                            e.printStackTrace();
                            return;
                        }
                        //调用自己的业务逻辑
                        try {
                            handler.handle(message);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }

    //内部抽象类 用于实现自己的处理逻辑
    public static abstract class MqMessageHandler<T extends Serializable>{
        public abstract void handle(T message);
    }

}
