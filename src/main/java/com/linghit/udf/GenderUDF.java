package com.linghit.udf;

/**
 * 0女；1男；2未知
 */
public class GenderUDF {

    public static String evaluate(String gender) {

        String result = "未知";

        if (gender == null || gender.equals(""))
            return result;

        if (gender.equals("0")) {
            result = "女";
        } else if (gender.equals("1")) {
            result = "男";
        } else if (gender.equals("男")) {
            result = "男";
        } else if (gender.equals("女")) {
            result = "女";
        } else if (gender.equals("male")) {
            result = "男";
        } else if (gender.equals("female")) {
            result = "女";
        }

        return result;
    }

}
