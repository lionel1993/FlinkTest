package com.linghit.udf;

import com.linghit.util.TextUtils;

public class PayResultUDF {

    /**
     * 解析支付结果
     */
    public static Integer evaluate(String pay_result_str) {

        if (TextUtils.isEmpty(pay_result_str))
            return 0;

        int pay_result = 0;

        if (pay_result_str.equals("1") || pay_result_str.equals("true")) {
            pay_result = 1;
        }

        return pay_result;
    }

}
