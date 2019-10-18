package utils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class GetDate {

    //获取某一天的前n天所在区间
    public static String getDayAgoDateDay(String day,int n) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String time = null;
        try {
            Date date = sdf.parse(day);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.add(Calendar.DAY_OF_YEAR,-n);
            time = sdf.format(calendar.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }   
        return time;
    }

    //获取当前时间前n天所在区间
    public static String getAgoDateDay(int n) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DATE, -n);
        Date date = calendar.getTime();
        String time = sdf.format(date);
        return time;
    }

    //获取当前时间前n周所在区间
    public static String getAgoDateWeek(int n) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar1 = Calendar.getInstance();
        Calendar calendar2 = Calendar.getInstance();
        int dayOfWeek = calendar1.get(Calendar.DAY_OF_WEEK) - 1;
        int offset1 = -dayOfWeek;
        int offset2 = 6 - dayOfWeek;
        calendar1.add(Calendar.DATE, offset1 - 7 * n);
        calendar2.add(Calendar.DATE, offset2 - 7 * n);
        String lastBeginDate = sdf.format(calendar1.getTime());
        String lastEndDate = sdf.format(calendar2.getTime());
        String returnDate = lastBeginDate + "~" + lastEndDate;
        return returnDate;
    }


    //获取当前时间前n月所在区间
    public static String getAgoDateMonth(int n) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.MONTH, -n);
        //设置当月第一天
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        String start = sdf.format(calendar.getTime());
        //设置为当月最后一天
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        String end = sdf.format(calendar.getTime());
        return start + "~" + end;
    }


    //获取传入时间前n月所在时间范围
    public static String getNMonthAgo(String st, int n) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        //获取当前时间前n月所在区间
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(sdf.parse(st));
        calendar.add(Calendar.MONTH, -n);
        //设置当月第一天
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        String start = sdf.format(calendar.getTime());
        //设置为当月最后一天
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        String end = sdf.format(calendar.getTime());
        return start + "~" + end;

    }

    //获取当前时间
    public static String getCurrentTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String current = sdf.format(new Date());
        return current;
    }


    //获取前n小时
    public static String getAgoDateHour(int n) {
        SimpleDateFormat sdf = new SimpleDateFormat("HH");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.HOUR, -n);
        Date date = calendar.getTime();
        String hour = sdf.format(date);
        return hour;
    }

    // 获取n小时前的时间
    public static String get_n_hour_ago(int n) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.HOUR, -n);
        Date date = calendar.getTime();
        String hour = sdf.format(date)+":00:00";
        return hour;
    }


    //获得当前时间的所有小时
    public static List<String> getHourList(){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH");
        int hour = Integer.valueOf(simpleDateFormat.format(new Date()));
        List list = new ArrayList();
        for(int i=0;i<=hour;i++){
            String hours = String.valueOf(i);
            if(hours.length()==1){
                hours = "0"+hours;
            }
            list.add(hours);
        }
        return list;
    }

    //获得以前时间的所有小时
    public static List<String> getHourList(String old){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH");
        SimpleDateFormat simpleDateFormatDay = new SimpleDateFormat("yyyy-MM-dd");
        String now_day = simpleDateFormatDay.format(new Date());
        int hour = Integer.valueOf(simpleDateFormat.format(new Date()));

        if(!old.equalsIgnoreCase(now_day)){
            hour = 23;
        }

        List list = new ArrayList();
        for(int i=0;i<=hour;i++){
            String hours = String.valueOf(i);
            if(hours.length()==1){
                hours = "0"+hours;
            }
            list.add(hours);
        }
        return list;
    }


    //日期格式转化 26/Jul/2019:15:32:44----2019-07-26 15:32:44
    public static String transDate(String date) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        Date date1 = formatter.parse(date);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String trans_date = simpleDateFormat.format(date1);
        return trans_date;
    }

    //获取时间范围内所有时间
    public static List getDataRange(String startDate, String endDate) {
        List<String> lDate = new ArrayList<String>();
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date dBegin = sd.parse(startDate);
            Date dEnd = sd.parse(endDate);
            lDate.add(sd.format(dBegin));
            Calendar calBegin = Calendar.getInstance();
            // 使用给定的 Date 设置此 Calendar 的时间
            calBegin.setTime(dBegin);
            Calendar calEnd = Calendar.getInstance();
            // 使用给定的 Date 设置此 Calendar 的时间
            calEnd.setTime(dEnd);
            // 测试此日期是否在指定日期之后
            while (dEnd.after(calBegin.getTime())) {
                // 根据日历的规则，为给定的日历字段添加或减去指定的时间量
                calBegin.add(Calendar.DAY_OF_MONTH, 1);
                lDate.add(sd.format(calBegin.getTime()));
            }
        } catch (Exception e) {
            System.out.println(e);
        }
        return lDate;
    }

    public static String getDateDelta(String date, int day) {
        /**
         * 获取指定天数的 +/- n天
         */
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String res = "";
        try {
            Date date1 = simpleDateFormat.parse(date);
            calendar.setTime(date1);
            calendar.add(Calendar.DATE, day);
            res = simpleDateFormat.format(calendar.getTime());
            return res;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }

  /*  public static Date string2Timestamp (String str) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(str);
    }*/

    public static void main(String[] args) throws ParseException {
//        System.out.println(getAgoDateWeek(0));
//        System.out.println(getAgoDateMonth(1));
//        System.out.println(getAgoDateMonth(2));
//        System.out.println(getAgoDateMonth(3));
//        System.out.println(getCurrentTime());
        try {
//            System.out.println(string2Timestamp("2019-09-08 19:57:58"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }






}
