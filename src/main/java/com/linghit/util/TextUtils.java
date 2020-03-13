package com.linghit.util;


public class TextUtils {

    public static boolean isEmpty(String str) {
        if (str == null) {
            return true;
        }
        str = str.trim();
        return str.length() == 0 || str.equals("null");
    }

    public static boolean isNotEmpty(String str) {
        return !TextUtils.isEmpty(str);
    }

}
