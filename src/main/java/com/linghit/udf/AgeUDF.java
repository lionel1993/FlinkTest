package com.linghit.udf;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 这个目前需要分平台
 */
public class AgeUDF {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");

    /**
     * @param birthday 生日,时间戳(秒)
     */
    public static Integer evaluate(Long birthday) {

        if (birthday == null) return null;

        try {
            long mills = birthday * 1000;

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(mills);

            return Calendar.getInstance().get(Calendar.YEAR) - calendar.get(Calendar.YEAR);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }
    public static Integer evaluate(String birthday){



        if(birthday == null || "".equals(birthday))
            return null;

        try{

            Date birthdayDate = sdf.parse(birthday);

            int age = new Date().getYear() - birthdayDate.getYear();

            return age;

            //防止解析异常
        }catch(Exception e){

            e.printStackTrace();

            return null;
        }

    }

    public static void main(String[] args) {
        System.out.println(evaluate(1234565677L));
    }
}
