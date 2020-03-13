package com.linghit.util;

import java.sql.Timestamp;

/**
 * 针对新的业务线规则进行处理，以'-'进行分隔，比如1-1
 */
public class AppUtil {

    /**
     * 获取一级业务线
     */
    public static String getApp(String app_id) {

        String app;

        if (app_id.contains("-")) {
            String[] arr = app_id.split("-");
            app = arr[0] + "-";
        } else {
            app = app_id.substring(0, 1);
        }

        return app + "00";
    }

    /**
     * 获取一级业务线
     */
    public static String getAppPrex(String app_id) {

        String app;

        if (app_id.contains("-")) {
            String[] arr = app_id.split("-");
            app = arr[0];
        } else {
            app = app_id.substring(0, 1);
        }

        return app;
    }

    /**
     * 比较版本大小
     * <p>
     * 说明：支n位基础版本号+1位子版本号
     * 示例：1.0.2>1.0.1 , 1.0.1.1>1.0.1
     *
     * @param version1 版本1
     * @param version2 版本2
     * @return 0:相同 1:version1大于version2 -1:version1小于version2
     */
    private static int compareVersion(String version1, String version2) {
        if (version1.equals(version2)) {
            return 0; //版本相同
        }
        String[] v1Array = version1.split("\\.");
        String[] v2Array = version2.split("\\.");
        int v1Len = v1Array.length;
        int v2Len = v2Array.length;
        int baseLen; //基础版本号位数（取长度小的）
        if (v1Len > v2Len) {
            baseLen = v2Len;
        } else {
            baseLen = v1Len;
        }

        for (int i = 0; i < baseLen; i++) { //基础版本号比较
            if (!v1Array[i].equals(v2Array[i]))  //同位版本号相同
                return Integer.parseInt(v1Array[i]) > Integer.parseInt(v2Array[i]) ? 1 : -1;

        }
        //基础版本相同，再比较子版本号
        if (v1Len != v2Len) {
            return v1Len > v2Len ? 1 : -1;
        } else {
            //基础版本相同，无子版本号
            return 0;
        }
    }

    /**
     * 大于等于
     * version是否大于等于base_version
     */
    public static boolean isLargeEqualVersion(String version, String base_version) {
        int compare = compareVersion(version, base_version);
        return compare == 0 || compare == 1;
    }


    public static void main(String[] args) {
        System.out.println(getApp("1-1"));
        System.out.println(getApp("C01"));
        System.out.println(getApp("A89"));
        System.out.println(getApp("58-1"));
        System.out.println(getApp("1-11"));
        System.out.println(new Timestamp(System.currentTimeMillis()));
        System.out.println("v1>v1:" + compareVersion("2.0", "1.0"));
        System.out.println(isLargeEqualVersion("2.0.1", "1.0.1.2"));
        System.out.println(isLargeEqualVersion("2.0", "3.0"));
    }

}
