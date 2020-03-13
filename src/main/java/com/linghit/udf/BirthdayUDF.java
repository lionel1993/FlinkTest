package com.linghit.udf;


import com.linghit.util.TextUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 对于生日时间戳解析，因为上传的日志格式不统一，所以只能大略进行处理
 * web:yyyyMMddHH和yyyy-MM-dd HH
 * app:时间戳(秒和毫秒都有可能)和yyyy-MM-dd HH
 */
public class BirthdayUDF {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
    private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * 生日时间戳解析
     *
     * @param birthday 生日
     * @param type     1是web，2是app
     * @return 返回秒数时间戳
     */
    public static Long evaluate(String birthday, int type) {

        if (TextUtils.isEmpty(birthday))
            return null;

        try {
            if (type == 1) {
                Date birthdayDate;
                if (birthday.contains("-")) {
                    birthdayDate = sdf2.parse(birthday);
                } else {
                    birthdayDate = sdf.parse(birthday);
                }

                return birthdayDate.getTime() / 1000;
            } else {
                long time;
                // 为了避免有2018-08-08这种格式
                if (birthday.contains("-") && !birthday.startsWith("-")) {
                    Date birthdayDate = sdf2.parse(birthday);
                    time = birthdayDate.getTime() / 1000;
                } else if (birthday.endsWith("000")) { // 毫秒情况下
                    String timeStr = birthday.substring(0, birthday.length() - 3);
                    time = Long.parseLong(timeStr);
                } else {
                    time = Long.parseLong(birthday);
                }

                return time;
            }
            //防止解析异常
        } catch (Exception e) {

            e.printStackTrace();

            return null;
        }

    }

    public static void main(String[] args) {
        System.out.println(evaluate("1992-01-22 13", 2));
    }
}
