package myflink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.DingTalkUtils;
import utils.KafkaUtil;

import java.lang.ref.SoftReference;
import java.text.ParseException;

import static utils.GetDate.getCurrentTime;
import static utils.GetDate.transDate;

/**
 * 医美曝光日志消费
 */
public class ExposureConsumer {

    public static Logger logger = LoggerFactory.getLogger(ExposureConsumer.class);

    public static  StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {

        int threadNum = 5;
//        System.out.println("启动" + args[0] + "个线程");
        KafkaUtil.startConsumer("yimei_ga_log_prod", "flink_test", new KafkaUtil.MqMessageHandler<String>() {
            //topic
            // :bihzLog    group.id:access_youzibuy
            @Override
            public void handle(String str) {
                //实现自己的处理逻辑
                try {
                parseGaAndInsertToDB(new SoftReference<String>(str));
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(),e);
                    DingTalkUtils.sendMessageAt("消费点击日志错误："+e.getMessage()+"-"+str,"17757147568");
                }
            }
        }, threadNum);
    }


    /**
     * 解析URL参数，并插入到CK的表中
     *
     * @param str
     */
    public static void parseGaAndInsertToDB(SoftReference<String> str) {
        String ck_table = "beauty_ga_access_parsed";
//        System.out.println(str.get());
        if (str == null || str.get() == null || "".equals(str.get())) return;
        String[] contentArray = str.get().split("Ξ", -1);
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
//                String myuid = contentArray[15];
//
//                if (null == myuid || "-".equals(myuid) || "".equals(myuid)) {
//                    myuid = "0";
//                }
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

                JSONObject requestBodyJson = JSONObject.parseObject(request_body);
                String page = requestBodyJson.getString("page");
                String version = requestBodyJson.getString("v");
                String device_id = requestBodyJson.getString("device_id");
                String platform = requestBodyJson.getString("platform");
                String is_beauty = requestBodyJson.getString("is_beauty");
                String app_id = requestBodyJson.getString("app_id");

                String myuid = requestBodyJson.getString("myuid");

                if (null == myuid || "-".equals(myuid) || "".equals(myuid)) {
                    myuid = "0";
                }

                String event = requestBodyJson.getString("event");
                if (event != null) {
                    event = event.replace("'", "\\\\'");
                }
                String source_page = requestBodyJson.getString("source_page");
                String body = requestBodyJson.getString("body");
                String action = requestBodyJson.getString("action");
                String type = requestBodyJson.getString("type");
//                if(StringUtils.isNotEmpty(body)){
//                    JSONObject bodyJson = JSONObject.parseObject(body);
//                    action = bodyJson.getString("action");
//                    type = bodyJson.getString("type");
//                }

                String duration = requestBodyJson.getString("duration");        //曝光时长，单位：s


                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("'" + app_id + "',");
                stringBuilder.append("'" + myuid + "',");
                stringBuilder.append("'" + access_time + "',");
                stringBuilder.append("'" + ip + "',");
                stringBuilder.append("'" + method + "',");
                stringBuilder.append("'" + client_id + "',");
                stringBuilder.append("'" + client_ver + "',");
                stringBuilder.append("'" + client_platform + "',");
                stringBuilder.append("'" + client_channel + "',");
                stringBuilder.append("'" + client_save + "',");
                stringBuilder.append("'" + myclient + "',");
                stringBuilder.append("'" + ua + "',");
                stringBuilder.append("'" + os_v + "',");
                stringBuilder.append("'" + idfa + "',");
                stringBuilder.append("'" + imei + "',");
                stringBuilder.append("'" + mac + "',");
                stringBuilder.append("'" + imsi + "',");
                stringBuilder.append("'" + ot + "',");
                stringBuilder.append("'" + apn + "',");
                stringBuilder.append("'" + openudid + "',");
                stringBuilder.append("'" + orders + "',");
                stringBuilder.append("'" + sw + "',");
                stringBuilder.append("'" + sh + "',");
                stringBuilder.append("'" + request_body + "',");
                stringBuilder.append("'" + path + "',");
                stringBuilder.append("'" + request + "',");        //request
                stringBuilder.append("'" + status + "',");
                stringBuilder.append("'" + body_bytes_sent + "',");
                stringBuilder.append("'" + content_length + "',");
                stringBuilder.append("'" + http_ver + "',");
                stringBuilder.append("'" + session_id + "',");
                stringBuilder.append("'" + from_type + "',");
                stringBuilder.append("'" + mode + "',");
                stringBuilder.append("'" + androidid + "',");
                stringBuilder.append("'" + access_time + "',");  //report_time=access_time
                stringBuilder.append("'" + page + "',");
                stringBuilder.append("'" + version + "',");
                stringBuilder.append("'" + device_id + "',");
                stringBuilder.append("'" + platform + "',");
                stringBuilder.append("'" + is_beauty + "',");
                stringBuilder.append("'" + source_page + "',");
                stringBuilder.append("'" + event + "',");
                stringBuilder.append("'" + action + "',");
                stringBuilder.append("'" + type + "',");
                stringBuilder.append("'" + duration + "',");
                stringBuilder.append("'" + getCurrentTime() + "',");


                String column = "app_id,myuid,access_time,ip,method,client_id,client_ver,client_platform," +
                        "client_channel,client_save,myclient,ua,os_v,idfa,imei,mac,imsi,ot,apn,openudid,orders,sw,sh," +
                        "request_body,path,request,status,body_bytes_sent,content_length,http_ver,session_id," +
                        "from_type,mode,androidid,report_time,page,version,device_id,platform,is_beauty,source_page," +
                        "event,action,type,duration,updated_at";


                if (stringBuilder.length() >= 1) {
                    String value = stringBuilder.substring(0, stringBuilder.length() - 1);
                    streamEnv.fromElements(value);

//                    String sql = "insert into table  %s (%s) values (%s)";
//                    ClickHouseUtil.execute(String.format(sql, ck_table, column, value));
                }


            }

        }

    }

}
