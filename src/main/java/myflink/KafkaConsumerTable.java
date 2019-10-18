package myflink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.util.Properties;

import static utils.GetDate.transDate;

public class KafkaConsumerTable {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kfk72.ecsmq.meiyoucloud.com:9092");
        properties.setProperty("group.id", "flink_test");

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, fsSettings);

        streamEnv.enableCheckpointing(5000);

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("yimei_ga_log_prod", new SimpleStringSchema(), properties);
        //默认消费策略
        consumer.setStartFromGroupOffsets();
        DataStream<String> stream = streamEnv.addSource(consumer).setParallelism(1);
        DataStream<String> stream_filter = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String line) throws Exception {
                if(line.split("Ξ", -1).length==30) {
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

                String[] contentArray = line.split("Ξ", -1);
                if (contentArray.length == 30) {
                    String ip = contentArray[0];
                    String access_time = null;
                    try {
                        access_time = transDate(contentArray[1].replace("[", "").replace("]", ""));
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    String request = contentArray[2];
                    String method = request.split(" ")[0];
                    String uri = request.split(" ")[1].split("\\?")[0];
                    String myclient = contentArray[3];

                    if (uri.equals("/beauty-log")) {

                        String http_ver = request.split(" ")[2];
                        String path = uri.split("\\?")[0];

                        String client_id = "";
                        String client_platform = "";
                        String client_ver = "";
                        String client_channel = "";
                        String client_save = "";
                        if (myclient.length() >= 2) {
                            client_id = myclient.substring(0, 2);
                        }
                        if (myclient.length() >= 3) {
                            client_platform = myclient.substring(2, 3);
                        }
                        if (myclient.length() >= 7) {
                            client_ver = myclient.substring(3, 7);
                        }
                        if (myclient.length() >= 11) {
                            client_channel = myclient.substring(7, 11);
                        }
                        if (myclient.length() >= 17) {
                            client_save = myclient.substring(11, 16);
                        }

                        String ua = contentArray[4];
                        String os_v = contentArray[5];
                        String idfa = contentArray[6];
                        String imei = contentArray[7];
                        String mac = contentArray[8];
                        String sw = contentArray[9];
                        String sh = contentArray[10];
                        String imsi = contentArray[11];
                        String ot = contentArray[12];
                        String apn = contentArray[13];
                        String openudid = contentArray[14];
                        String orders = contentArray[16];
                        String request_body = contentArray[17].replace("\\x22", "\"");
                        String status = contentArray[18];
                        String body_bytes_sent = contentArray[19];
                        String content_length = contentArray[20];
                        String session_id = contentArray[21];
                        String from_type = contentArray[22];
                        if (from_type == null || from_type.equals("-") || from_type.equals("")) {
                            from_type = "0";
                        } else {
                            from_type = "1";
                        }
                        String mode = contentArray[23];
                        String androidid = contentArray[24];
                        String maintab = contentArray[25];
                        String history = contentArray[27];
                        String ab_exp = contentArray[28];
                        String ab_isol = contentArray[29];



                        String page = "";
                        String version = "";
                        String device_id = "";
                        String platform = "";
                        String is_beauty = "";
                        String app_id = "0";
                        String myuid = "";
                        String event = "";
                        String source_page = "";
                        String body = "";
                        String action = "";
                        String type = "";
                        String duration = "" ;        //曝光时长，单位：s

                        JSONObject requestBodyJson = JSONObject.parseObject(request_body);

                        if(null != requestBodyJson){
                             page = requestBodyJson.getString("page");
                             version = requestBodyJson.getString("v");
                             device_id = requestBodyJson.getString("device_id");
                             platform = requestBodyJson.getString("platform");
                             is_beauty = requestBodyJson.getString("is_beauty");
                             app_id = requestBodyJson.getString("app_id");

                             myuid = requestBodyJson.getString("myuid");

                            if (null == myuid || "-".equals(myuid) || "".equals(myuid)) {
                                myuid = "0";
                            }

                             event = requestBodyJson.getString("event");
                            if (event != null) {
                                event = event.toString().replace("'", "\\\\'");
                            }
                             source_page = requestBodyJson.getString("source_page");
                             body = requestBodyJson.getString("body");
                             action = requestBodyJson.getString("action");
                             type = requestBodyJson.getString("type");

                             duration = requestBodyJson.getString("duration");        //曝光时长，单位：s

                        }else{
                            System.out.println(requestBodyJson);
                        }


                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("" + app_id + ",");
                        stringBuilder.append("" + myuid + ",");
                        stringBuilder.append("" + access_time + ",");
//                        stringBuilder.append("'" + ip + "',");
//                        stringBuilder.append("'" + method + "',");
//                        stringBuilder.append("'" + client_id + "',");
//                        stringBuilder.append("'" + client_ver + "',");
//                        stringBuilder.append("'" + client_platform + "',");
//                        stringBuilder.append("'" + client_channel + "',");
//                        stringBuilder.append("'" + client_save + "',");
//                        stringBuilder.append("'" + myclient + "',");
//                        stringBuilder.append("'" + ua + "',");
//                        stringBuilder.append("'" + os_v + "',");
//                        stringBuilder.append("'" + idfa + "',");
//                        stringBuilder.append("'" + imei + "',");
//                        stringBuilder.append("'" + mac + "',");
//                        stringBuilder.append("'" + imsi + "',");
//                        stringBuilder.append("'" + ot + "',");
//                        stringBuilder.append("'" + apn + "',");
//                        stringBuilder.append("'" + openudid + "',");
//                        stringBuilder.append("'" + orders + "',");
//                        stringBuilder.append("'" + sw + "',");
//                        stringBuilder.append("'" + sh + "',");
//                        stringBuilder.append("'" + request_body + "',");
//                        stringBuilder.append("'" + path + "',");
//                        stringBuilder.append("'" + request + "',");        //request
//                        stringBuilder.append("'" + status + "',");
//                        stringBuilder.append("'" + body_bytes_sent + "',");
//                        stringBuilder.append("'" + content_length + "',");
//                        stringBuilder.append("'" + http_ver + "',");
//                        stringBuilder.append("'" + session_id + "',");
//                        stringBuilder.append("'" + from_type + "',");
//                        stringBuilder.append("'" + mode + "',");
//                        stringBuilder.append("'" + androidid + "',");
//                        stringBuilder.append("'" + access_time + "',");  //report_time=access_time
//                        stringBuilder.append("'" + page + "',");
//                        stringBuilder.append("'" + version + "',");
//                        stringBuilder.append("'" + device_id + "',");
//                        stringBuilder.append("'" + platform + "',");
//                        stringBuilder.append("'" + is_beauty + "',");
//                        stringBuilder.append("'" + source_page + "',");
//                        stringBuilder.append("'" + event + "',");
//                        stringBuilder.append("'" + action + "',");
//                        stringBuilder.append("'" + type + "',");
//                        stringBuilder.append("'" + duration + "',");
//                        stringBuilder.append("'" + getCurrentTime() + "',");

                        if (stringBuilder.length() >= 1) {
                            String value = stringBuilder.substring(0, stringBuilder.length() - 1);
                            collector.collect(value);
                        }

                    }
                }
            }
        });
//        stream_flatMap.print();

        DataStream<Tuple3<String, String,String>> map =
                stream_flatMap.map(new MapFunction<String, Tuple3<String, String,String>>() {

            private static final long serialVersionUID = 1471936326697828381L;

            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple3<String, String, String>(split[0],split[1],split[2]);
            }
        });

        tableEnv.registerDataStream("beauty_ga_access_parsed", map,"app_id,myuid,access_time");
        Table ga_table = tableEnv.sqlQuery("select * from beauty_ga_access_parsed where " +
                "1=1  and app_id='1' and access_time>='2019-10-17 20:16:00' ");
//        DataStream<Tuple2<Boolean,Long>> result = tableEnv.toRetractStream(ga_table, Long.class);

        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(ga_table, Row.class);

        result.print();

        try {
            streamEnv.execute("demo");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




}
