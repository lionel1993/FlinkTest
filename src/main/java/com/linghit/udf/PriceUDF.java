package com.linghit.udf;

import com.linghit.util.TextUtils;

import java.util.HashMap;
import java.util.Map;

public class PriceUDF {

    private static final Map<String, Double> pricePrefixMap = new HashMap<>();

    static {
        // 币种前缀+汇率，比如put("HK$",0.8462)表示一港币=0.8462人民币
        pricePrefixMap.put("¥", 1.0);
        pricePrefixMap.put("HK$", 0.8462);
        pricePrefixMap.put("$", 6.5984);
        pricePrefixMap.put("NT$", 0.2207);
    }

    /**
     * 用于解析价格
     */
    public static Double evaluate(String priceStr) {

        if (TextUtils.isEmpty(priceStr))
            return null;

        try {
            Double price = null;

            priceStr = priceStr.trim();

            boolean contianPrefix = false;

            //包含币种前缀，则先去币种前缀，在根据汇率转换为人民币
            for (String prefix : pricePrefixMap.keySet()) {

                if (priceStr.startsWith(prefix)) {
                    priceStr = priceStr.replace(prefix, "").trim();

                    //计算汇率
                    price = Double.valueOf(priceStr) * pricePrefixMap.get(prefix);

                    contianPrefix = true;
                    break;
                }
            }

            //商城的价格会出现100;200这种形式
            if (priceStr.contains(";")) {
                String[] priceAttr = priceStr.split(";");
                double initPrice = 0;
                for (String p : priceAttr) {
                    initPrice += Double.valueOf(p);
                }
                if (initPrice > 0) price = initPrice;
            } else if (!contianPrefix) { // 如果不包含币种前缀，则直接转换
                price = Double.valueOf(priceStr);
            }

            return price;
        } catch (Exception e) {
            System.out.println("解析price出错：" + priceStr);
            return null;
        }

    }

}
