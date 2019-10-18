package utils;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiChatSendRequest;
import com.dingtalk.api.request.OapiRobotSendRequest;
import com.dingtalk.api.response.OapiChatSendResponse;
import com.dingtalk.api.response.OapiRobotSendResponse;

import java.util.Arrays;

/**
 * Created by cfbin on 2018/7/25.
 */
public class DingTalkUtils {
    public static String serverUrl="https://oapi.dingtalk.com/robot/send?";
    public static String token="6c393c133a45121d824828d919d478622f12692bdee9315f17ded2c079003efc";
    public static DingTalkClient client = null;

    /**
     *
     * @param txt 消息体
     */
    public static void sendMessage(String txt){
        client=new DefaultDingTalkClient(serverUrl);
        OapiChatSendRequest request = new OapiChatSendRequest();
        request.setMsgtype("text");
        OapiChatSendRequest.Text text = new OapiChatSendRequest.Text();
        //设置发送内容
        text.setContent(txt);

        request.setText(text);

        try {
            OapiChatSendResponse response = client.execute(request, token);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param txt 发送的消息体
     * @param phoneNum  要@的人手机号
     */
    public static void     sendMessageAt(String txt,String phoneNum){
        client=new DefaultDingTalkClient(serverUrl);
        OapiRobotSendRequest request = new OapiRobotSendRequest();



        request.setMsgtype("text");
        OapiRobotSendRequest.Text text = new OapiRobotSendRequest.Text();
        text.setContent(txt);


        request.setText(text);
        OapiRobotSendRequest.At at = new OapiRobotSendRequest.At();
        if(phoneNum !=null){
            at.setAtMobiles(Arrays.asList(phoneNum));
            request.setAt(at);
        }else {
            at.setIsAtAll("true");
            request.setAt(at);
        }

        try {
            OapiRobotSendResponse response = client.execute(request,token);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     *
     * @param txt
     * @param phoneNums 数组存多个人手机号可以@多个人，若phoneNums=null则@所有人
     */
    public static void sendMessageAtPhones(String txt,String[] phoneNums){
        client=new DefaultDingTalkClient(serverUrl);
        OapiRobotSendRequest request = new OapiRobotSendRequest();



        request.setMsgtype("text");
        OapiRobotSendRequest.Text text = new OapiRobotSendRequest.Text();
        text.setContent(txt);


        request.setText(text);
        OapiRobotSendRequest.At at = new OapiRobotSendRequest.At();
        if(phoneNums !=null){
            at.setAtMobiles(Arrays.asList(phoneNums));
            request.setAt(at);
        }else {
            at.setIsAtAll("true");
            request.setAt(at);
        }

        try {
            OapiRobotSendResponse response = client.execute(request,token);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
