package uvpv;


import java.text.SimpleDateFormat;
import java.util.Date;

/*
基于ThreadLocal的线程安全日期工具类

by lionel

2020-03-12
 */
public class DateUtils {

    private static ThreadLocal<SimpleDateFormat> sdfTime = new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

    private static ThreadLocal<SimpleDateFormat> sdfDate = new ThreadLocal<SimpleDateFormat>(){
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };

    /*
    解析时间戳为时间字符串
     */
    public static String parseTimestampToTimeStr(long timestamp){
        try {
            return sdfTime.get().format(new Date(timestamp));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /*
    解析时间戳为日期字符串
  */
    public static String parseTimestampToDateStr(long timestamp){
        try {
            return sdfDate.get().format(new Date(timestamp));
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    /*
    解析时间字符串为时间戳
    */
    public  static long parseTimeStrToTimestamp(String timeStr){
        try {
            return sdfTime.get().parse(timeStr).getTime();
        }catch (Exception e){
            e.printStackTrace();
            return 0L;
        }

    }

    /*
    解析日期字符串为时间戳
    */
    public  static long parseDateStrToTimestamp(String dateStr){
        try {
            return sdfDate.get().parse(dateStr).getTime();
        }catch (Exception e){
            e.printStackTrace();
            return 0L;
        }

    }

}
