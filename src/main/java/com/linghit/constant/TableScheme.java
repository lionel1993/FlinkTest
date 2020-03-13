package com.linghit.constant;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;

import java.util.LinkedHashMap;
import java.util.Map;

public class TableScheme {

    /**
     * 在线测试的spark用户表结构
     */
    public static Map<String, Type> zxcs_info_map = new LinkedHashMap<>();

    /**
     * 在线测试的spark事件表结构
     */
    public static Map<String, Type> zxcs_flow_map = new LinkedHashMap<>();

    /**
     * 在线测试的统一平台用户表结构
     */
    public static Map<String, Type> linghit_all_info_map = new LinkedHashMap<>();
    public static Map<String, Type> linghit_sub_info_map = new LinkedHashMap<>();
    public static Map<String, Type> linghit_flow_map = new LinkedHashMap<>();

    static {
        zxcs_info_table();
        zxcs_flow_table();
        
        linghit_all_info_table();
        linghit_sub_info_table();
        linghit_flow_table();
    }


    private static void zxcs_info_table() {
        zxcs_info_map.put("key::app_id", Type.STRING);
        zxcs_info_map.put("key::user_id", Type.STRING);
        zxcs_info_map.put("key::start_time", Type.INT32);
        zxcs_info_map.put("type", Type.STRING);
        zxcs_info_map.put("$app_version", Type.STRING);
        zxcs_info_map.put("$channel", Type.STRING);
        zxcs_info_map.put("$channel_name", Type.STRING);
        zxcs_info_map.put("$channel_group", Type.STRING);
        zxcs_info_map.put("$channel_group_name", Type.STRING);
        zxcs_info_map.put("$app_userid", Type.STRING);
        zxcs_info_map.put("$openid", Type.STRING);
        zxcs_info_map.put("$token_person", Type.STRING);
        zxcs_info_map.put("$token_umeng", Type.STRING);
        zxcs_info_map.put("$username", Type.STRING);
        zxcs_info_map.put("$alias", Type.STRING);
        zxcs_info_map.put("$birthday", Type.STRING);
        zxcs_info_map.put("$age", Type.INT8);
        zxcs_info_map.put("$gender", Type.STRING);
        zxcs_info_map.put("$ip", Type.STRING);
        zxcs_info_map.put("$city", Type.STRING);
        zxcs_info_map.put("$province", Type.STRING);
        zxcs_info_map.put("$country", Type.STRING);
        zxcs_info_map.put("$mail", Type.STRING);
        zxcs_info_map.put("$phone", Type.STRING);
        zxcs_info_map.put("$qq", Type.STRING);
        zxcs_info_map.put("$wechat", Type.STRING);
        zxcs_info_map.put("$work_status", Type.STRING);
        zxcs_info_map.put("$profession", Type.STRING);
        zxcs_info_map.put("$marital_status", Type.STRING);
        zxcs_info_map.put("$equipment_brand", Type.STRING);
        zxcs_info_map.put("$equipment_brand_name", Type.STRING);
        zxcs_info_map.put("$equipment_code", Type.STRING);
        zxcs_info_map.put("$sys", Type.STRING);
        zxcs_info_map.put("$sys_version", Type.STRING);
        zxcs_info_map.put("$browser", Type.STRING);
        zxcs_info_map.put("$screen_width", Type.STRING);
        zxcs_info_map.put("$screen_height", Type.STRING);
        zxcs_info_map.put("$screen_size", Type.STRING);
        zxcs_info_map.put("$network", Type.STRING);
        zxcs_info_map.put("$wifi_switch", Type.STRING);
        zxcs_info_map.put("$phone_operator", Type.STRING);
        zxcs_info_map.put("$phone_operator_name", Type.STRING);
        zxcs_info_map.put("regist_time", Type.INT32);
        zxcs_info_map.put("row_date", Type.STRING);
        zxcs_info_map.put("schannel", Type.STRING);
    }


    private static void zxcs_flow_table() {
        zxcs_flow_map.put("key::log_id", Type.STRING);
        zxcs_flow_map.put("key::app_id", Type.STRING);
        zxcs_flow_map.put("key::log_time", Type.INT32);
        zxcs_flow_map.put("type", Type.STRING);
        zxcs_flow_map.put("$app_version", Type.STRING);
        zxcs_flow_map.put("log_type", Type.STRING);
        zxcs_flow_map.put("row_date", Type.STRING);
        zxcs_flow_map.put("user_id", Type.STRING);
        zxcs_flow_map.put("original_id", Type.STRING);
        zxcs_flow_map.put("event_id", Type.STRING);
        zxcs_flow_map.put("event_name", Type.STRING);
        zxcs_flow_map.put("$channel", Type.STRING);
        zxcs_flow_map.put("$channel_name", Type.STRING);
        zxcs_flow_map.put("$channel_group", Type.STRING);
        zxcs_flow_map.put("$channel_group_name", Type.STRING);
        zxcs_flow_map.put("$ip", Type.STRING);
        zxcs_flow_map.put("$city", Type.STRING);
        zxcs_flow_map.put("$province", Type.STRING);
        zxcs_flow_map.put("$country", Type.STRING);
        zxcs_flow_map.put("$useragent", Type.STRING);
        zxcs_flow_map.put("$referrer", Type.STRING);
        zxcs_flow_map.put("$equipment_brand", Type.STRING);
        zxcs_flow_map.put("$equipment_brand_name", Type.STRING);
        zxcs_flow_map.put("$equipment_code", Type.STRING);
        zxcs_flow_map.put("$sys", Type.STRING);
        zxcs_flow_map.put("$sys_version", Type.STRING);
        zxcs_flow_map.put("$browser", Type.STRING);
        zxcs_flow_map.put("$network", Type.STRING);
        zxcs_flow_map.put("$phone_operator", Type.STRING);
        zxcs_flow_map.put("$phone_operator_name", Type.STRING);
        zxcs_flow_map.put("$regist_way", Type.STRING);
        zxcs_flow_map.put("$login_way", Type.STRING);
        zxcs_flow_map.put("$url", Type.STRING);
        zxcs_flow_map.put("$title", Type.STRING);
        zxcs_flow_map.put("$scan_time", Type.STRING);
        zxcs_flow_map.put("$module", Type.STRING);
        zxcs_flow_map.put("$start_way", Type.STRING);
        zxcs_flow_map.put("$order_id", Type.STRING);
        zxcs_flow_map.put("$order_price", Type.DOUBLE);
        zxcs_flow_map.put("$goods_name", Type.STRING);
        zxcs_flow_map.put("$goods_price", Type.STRING);
        zxcs_flow_map.put("$goods_amount", Type.STRING);
        zxcs_flow_map.put("$pay_way", Type.STRING);
        zxcs_flow_map.put("$pay_price", Type.DOUBLE);
        zxcs_flow_map.put("$pay_result", Type.INT8);
        zxcs_flow_map.put("schannel", Type.STRING);
    }

    private static void linghit_all_info_table() {
        linghit_all_info_map.put("key::linghit_id", Type.STRING);
        linghit_all_info_map.put("key::first_time", Type.INT32);
        linghit_all_info_map.put("key::app_id", Type.STRING);
        linghit_all_info_map.put("app", Type.STRING);
        linghit_all_info_map.put("cookie", Type.STRING);
        linghit_all_info_map.put("openid", Type.STRING);
        linghit_all_info_map.put("$equipment_id", Type.STRING);
        linghit_all_info_map.put("$phone", Type.STRING);
        linghit_all_info_map.put("$mail", Type.STRING);
        linghit_all_info_map.put("first_use_time", Type.INT32);
        linghit_all_info_map.put("first_regist_time", Type.INT32);
        linghit_all_info_map.put("first_login_time", Type.INT32);
        linghit_all_info_map.put("first_consume_time", Type.INT32);
        linghit_all_info_map.put("$app_version", Type.STRING);
        linghit_all_info_map.put("$channel", Type.STRING);
        linghit_all_info_map.put("$channel_name", Type.STRING);
        linghit_all_info_map.put("$channel_group", Type.STRING);
        linghit_all_info_map.put("$channel_group_name", Type.STRING);
        linghit_all_info_map.put("$app_userid", Type.STRING);
        linghit_all_info_map.put("$token_person", Type.STRING);
        linghit_all_info_map.put("$token_umeng", Type.STRING);
        linghit_all_info_map.put("$username", Type.STRING);
        linghit_all_info_map.put("$alias", Type.STRING);
        linghit_all_info_map.put("$birthday", Type.STRING);
        linghit_all_info_map.put("$age", Type.INT16);
        linghit_all_info_map.put("$gender", Type.STRING);
        linghit_all_info_map.put("$ip", Type.STRING);
        linghit_all_info_map.put("$city", Type.STRING);
        linghit_all_info_map.put("$province", Type.STRING);
        linghit_all_info_map.put("$country", Type.STRING);
        linghit_all_info_map.put("$qq", Type.STRING);
        linghit_all_info_map.put("$wechat", Type.STRING);
        linghit_all_info_map.put("$work_status", Type.STRING);
        linghit_all_info_map.put("$profession", Type.STRING);
        linghit_all_info_map.put("$marital_status", Type.STRING);
        linghit_all_info_map.put("$equipment_brand", Type.STRING);
        linghit_all_info_map.put("$equipment_brand_name", Type.STRING);
        linghit_all_info_map.put("$equipment_code", Type.STRING);
        linghit_all_info_map.put("$sys", Type.STRING);
        linghit_all_info_map.put("$sys_version", Type.STRING);
        linghit_all_info_map.put("$browser", Type.STRING);
        linghit_all_info_map.put("$screen_width", Type.STRING);
        linghit_all_info_map.put("$screen_height", Type.STRING);
        linghit_all_info_map.put("$screen_size", Type.STRING);
        linghit_all_info_map.put("$network", Type.STRING);
        linghit_all_info_map.put("$wifi_switch", Type.STRING);
        linghit_all_info_map.put("$phone_operator", Type.STRING);
        linghit_all_info_map.put("$phone_operator_name", Type.STRING);
        linghit_all_info_map.put("$token_phone", Type.STRING);
        linghit_all_info_map.put("$push_brand", Type.STRING);
    }


    private static void linghit_sub_info_table() {
        linghit_sub_info_map.put("key::linghit_id", Type.STRING);
        linghit_sub_info_map.put("key::first_time", Type.INT32);
        linghit_sub_info_map.put("key::app_id", Type.STRING);
        linghit_sub_info_map.put("app", Type.STRING);
        linghit_sub_info_map.put("cookie", Type.STRING);
        linghit_sub_info_map.put("openid", Type.STRING);
        linghit_sub_info_map.put("$equipment_id", Type.STRING);
        linghit_sub_info_map.put("$phone", Type.STRING);
        linghit_sub_info_map.put("$mail", Type.STRING);
        linghit_sub_info_map.put("first_use_time", Type.INT32);
        linghit_sub_info_map.put("first_regist_time", Type.INT32);
        linghit_sub_info_map.put("first_login_time", Type.INT32);
        linghit_sub_info_map.put("first_consume_time", Type.INT32);
        linghit_sub_info_map.put("$app_version", Type.STRING);
        linghit_sub_info_map.put("$channel", Type.STRING);
        linghit_sub_info_map.put("$channel_name", Type.STRING);
        linghit_sub_info_map.put("$channel_group", Type.STRING);
        linghit_sub_info_map.put("$channel_group_name", Type.STRING);
        linghit_sub_info_map.put("$app_userid", Type.STRING);
        linghit_sub_info_map.put("$token_person", Type.STRING);
        linghit_sub_info_map.put("$token_umeng", Type.STRING);
        linghit_sub_info_map.put("$username", Type.STRING);
        linghit_sub_info_map.put("$alias", Type.STRING);
        linghit_sub_info_map.put("$birthday", Type.STRING);
        linghit_sub_info_map.put("$age", Type.INT16);
        linghit_sub_info_map.put("$gender", Type.STRING);
        linghit_sub_info_map.put("$ip", Type.STRING);
        linghit_sub_info_map.put("$city", Type.STRING);
        linghit_sub_info_map.put("$province", Type.STRING);
        linghit_sub_info_map.put("$country", Type.STRING);
        linghit_sub_info_map.put("$qq", Type.STRING);
        linghit_sub_info_map.put("$wechat", Type.STRING);
        linghit_sub_info_map.put("$work_status", Type.STRING);
        linghit_sub_info_map.put("$profession", Type.STRING);
        linghit_sub_info_map.put("$marital_status", Type.STRING);
        linghit_sub_info_map.put("$equipment_brand", Type.STRING);
        linghit_sub_info_map.put("$equipment_brand_name", Type.STRING);
        linghit_sub_info_map.put("$equipment_code", Type.STRING);
        linghit_sub_info_map.put("$sys", Type.STRING);
        linghit_sub_info_map.put("$sys_version", Type.STRING);
        linghit_sub_info_map.put("$browser", Type.STRING);
        linghit_sub_info_map.put("$screen_width", Type.STRING);
        linghit_sub_info_map.put("$screen_height", Type.STRING);
        linghit_sub_info_map.put("$screen_size", Type.STRING);
        linghit_sub_info_map.put("$network", Type.STRING);
        linghit_sub_info_map.put("$wifi_switch", Type.STRING);
        linghit_sub_info_map.put("$phone_operator", Type.STRING);
        linghit_sub_info_map.put("$phone_operator_name", Type.STRING);
        linghit_sub_info_map.put("$token_phone", Type.STRING);
        linghit_sub_info_map.put("$push_brand", Type.STRING);
    }



    private static void linghit_flow_table() {
        linghit_flow_map.put("key::log_id", Type.STRING);
        linghit_flow_map.put("key::log_time", Type.INT32);
        linghit_flow_map.put("key::app_id", Type.STRING);
        linghit_flow_map.put("app", Type.STRING);
        linghit_flow_map.put("linghit_id", Type.STRING);
        linghit_flow_map.put("$app_version", Type.STRING);
        linghit_flow_map.put("log_type", Type.STRING);
        linghit_flow_map.put("user_id", Type.STRING);
        linghit_flow_map.put("original_id", Type.STRING);
        linghit_flow_map.put("event_id", Type.STRING);
        linghit_flow_map.put("event_name", Type.STRING);
        linghit_flow_map.put("$channel", Type.STRING);
        linghit_flow_map.put("$channel_name", Type.STRING);
        linghit_flow_map.put("$channel_group", Type.STRING);
        linghit_flow_map.put("$channel_group_name", Type.STRING);
        linghit_flow_map.put("$ip", Type.STRING);
        linghit_flow_map.put("$city", Type.STRING);
        linghit_flow_map.put("$province", Type.STRING);
        linghit_flow_map.put("$country", Type.STRING);
        linghit_flow_map.put("$useragent", Type.STRING);
        linghit_flow_map.put("$referrer", Type.STRING);
        linghit_flow_map.put("$equipment_brand", Type.STRING);
        linghit_flow_map.put("$equipment_brand_name", Type.STRING);
        linghit_flow_map.put("$equipment_code", Type.STRING);
        linghit_flow_map.put("$sys", Type.STRING);
        linghit_flow_map.put("$sys_version", Type.STRING);
        linghit_flow_map.put("$browser", Type.STRING);
        linghit_flow_map.put("$network", Type.STRING);
        linghit_flow_map.put("$phone_operator", Type.STRING);
        linghit_flow_map.put("$phone_operator_name", Type.STRING);
        linghit_flow_map.put("$regist_way", Type.STRING);
        linghit_flow_map.put("$login_way", Type.STRING);
        linghit_flow_map.put("$url", Type.STRING);
        linghit_flow_map.put("$title", Type.STRING);
        linghit_flow_map.put("$scan_time", Type.STRING);
        linghit_flow_map.put("$module", Type.STRING);
        linghit_flow_map.put("$start_way", Type.STRING);
        linghit_flow_map.put("$order_id", Type.STRING);
        linghit_flow_map.put("$order_price", Type.DOUBLE);
        linghit_flow_map.put("$goods_name", Type.STRING);
        linghit_flow_map.put("$goods_price", Type.DOUBLE);
        linghit_flow_map.put("$goods_amount", Type.STRING);
        linghit_flow_map.put("$pay_way", Type.STRING);
        linghit_flow_map.put("$pay_price", Type.DOUBLE);
        linghit_flow_map.put("$pay_result", Type.INT8);
        linghit_flow_map.put("ship", Type.STRING);
        linghit_flow_map.put("discount", Type.STRING);
        linghit_flow_map.put("address", Type.STRING);
        linghit_flow_map.put("$star_id", Type.STRING);
        linghit_flow_map.put("row_date", Type.STRING);
    }
}
