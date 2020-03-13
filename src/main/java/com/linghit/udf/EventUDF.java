package com.linghit.udf;


import java.util.HashMap;
import java.util.Map;

/**
 * 此UDF清洗事件id和事件名之间的映射
 */
public class EventUDF {

    private static Map<String, String> eventNameMap = new HashMap<>();

    static {
        eventNameMap.put("$Regist", "注册");
        eventNameMap.put("$EditProfile", "修改资料");
        eventNameMap.put("$Login", "登录");
        eventNameMap.put("$Logout", "注销");
        eventNameMap.put("$Scan", "浏览");
        eventNameMap.put("$EnterPage", "进入页面");
        eventNameMap.put("enterPage", "进入页面");
        eventNameMap.put("$LeavePage", "离开页面");
        eventNameMap.put("leavePage", "离开页面");
        eventNameMap.put("$Click", "点击");
        eventNameMap.put("$Order", "下单");
        eventNameMap.put("$OrderDetail", "下单详情");
        eventNameMap.put("$TurnUpPay", "调起支付");
        eventNameMap.put("$Pay", "支付");
        eventNameMap.put("$PayDetail", "支付详情");
        eventNameMap.put("$EndApp", "离开应用");
        eventNameMap.put("$StartApp", "进入应用");
        eventNameMap.put("$BackApp", "进入后台");
        eventNameMap.put("$FrontApp", "进入前台");
    }

    /**
     * type : 1表示返回event_id，其余返回名称
     */
    public static String evaluate(String id) {

        return eventNameMap.get(id);
    }

}
