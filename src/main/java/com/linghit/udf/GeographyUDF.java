package com.linghit.udf;

import com.linghit.util.TextUtils;

public class GeographyUDF {

    /**
     * @param geography 中文城市省份国家名称
     */
    public static String evaluate(String geography) {

        if (TextUtils.isEmpty(geography))
            return "未知";

        if (geography.endsWith("省") || geography.endsWith("市"))
            geography = geography.substring(0, geography.length() - 1);

        return geography;
    }

}
