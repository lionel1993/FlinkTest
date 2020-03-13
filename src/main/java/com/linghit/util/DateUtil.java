package com.linghit.util;

import com.linghit.udf.EventUDF;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by ygz on 2019/5/23.
 *
 *
 * 注意：SimpleDateFormat线程不安全
 *
 */
public class DateUtil {

    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");


    public static void main(String[] args) {
        System.out.println(parseTime("1560768003"));

        System.out.println(EventUDF.evaluate("$Scan"));

        System.out.println(getPartitionMonthTime("2016-01-01", "2020-03-01"));
    }


    /**
     * 时间戳转换日期
     *
     * @param second 秒
     */
    public static String parseTime(String second) {

        String date;
        try {
            long time = Long.parseLong(second);
            date = format.format(new Date(time * 1000));
        } catch (Exception e) {
            date = format.format(new Date());
        }
        return date;
    }

    public static Timestamp getTimeStamp(String tsStr) {
        Timestamp ts = new Timestamp(System.currentTimeMillis());

        try {
            ts = Timestamp.valueOf(tsStr);
            System.out.println(ts);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ts;
    }

    /**
     * 获取完整的日期
     *
     * @param second 秒
     */
    public static String getFullDateFromTimeStamp(String second) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date_str = "";
        try {
            long time = Long.parseLong(second);
            date_str = simpleDateFormat.format(time * 1000);

        } catch (Exception ex) {
            System.out.println("String转换Date错误，请确认数据可以转换！");
        }
        return date_str;
    }

    /**
     * 日期格式字符串转换成时间戳
     *
     * @param dateTime 日期
     * @return 时间戳(秒)
     */
    public static long date2TimeStamp(String dateTime) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = sdf.parse(dateTime);

            return date.getTime() / 1000;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 日期格式字符串转换成时间戳
     *
     * @param dateTime 日期
     * @return 时间戳(秒)
     */
    public static int date2TimeStampForKudu(String dateTime) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = sdf.parse(dateTime);

            return (int) (date.getTime() / 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }


    /**
     * 获取推进月份
     *
     * @return 返回秒
     */
    public static String getAddMonth(String dateTime, int months) {
        Calendar calendar = Calendar.getInstance();

        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date start_date = sdf.parse(dateTime);
            calendar.setTime(start_date);
            calendar.add(Calendar.MONTH, months);

            return sdf.format(calendar.getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }


    public static List<String> getPartitionMonthTime(String startDateTime, String endDateTime) {
        System.out.println(getAddMonth(startDateTime, 1));

        String start_date = startDateTime;
        String end_date = null;
        List<String> list = new ArrayList<>();
        while (!start_date.equals(endDateTime)) {
            end_date = getAddMonth(start_date, 1);
            list.add(start_date + ":" + end_date);
            start_date = end_date;
        }
        return list;
    }


    /**
     * 当前日期
     */
    public static String parseCurrentTime() {

        String date;
        try {
            date = format.format(new Date());
        } catch (Exception e) {
            date = format.format(new Date());
        }
        return date;
    }


}
