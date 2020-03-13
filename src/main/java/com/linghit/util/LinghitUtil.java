package com.linghit.util;

import com.alibaba.fastjson.JSONObject;
import com.linghit.constant.Constant;
import com.linghit.domain.*;
import com.linghit.udf.*;

import java.util.*;

public class LinghitUtil {

    /**
     * 获取AllUserInfo Domain
     */
    public static AllUserInfo getAllUserInfo(JSONObject object) {
        AllUserInfo allUserInfo = new AllUserInfo();
        JSONObject attrObject = object.getJSONObject(Constant.LINGHIT.attr);

        allUserInfo.setApp_id(object.getString(Constant.LINGHIT.app_id));
        allUserInfo.set$cookie(attrObject.getString(Constant.LINGHIT.cookie));
        allUserInfo.set$openid(attrObject.getString(Constant.LINGHIT.openid));
        allUserInfo.set$equipment_id(attrObject.getString(Constant.LINGHIT.$equipment_id));
        allUserInfo.set$phone(attrObject.getString(Constant.LINGHIT.$phone));
        allUserInfo.set$app_version(attrObject.getString(Constant.LINGHIT.$app_version));
        allUserInfo.set$channel(attrObject.getString(Constant.LINGHIT.$channel));
        allUserInfo.set$ip(attrObject.getString(Constant.LINGHIT.$ip));
        allUserInfo.set$city(attrObject.getString(Constant.LINGHIT.$city));
        allUserInfo.set$province(attrObject.getString(Constant.LINGHIT.$province));
        allUserInfo.set$country(attrObject.getString(Constant.LINGHIT.$country));
        allUserInfo.set$screen_width(attrObject.getString(Constant.LINGHIT.$screen_width));
        allUserInfo.set$screen_height(attrObject.getString(Constant.LINGHIT.$screen_height));
        allUserInfo.set$screen_size(attrObject.getString(Constant.LINGHIT.$screen_size));


        allUserInfo.setLinghit_id(object.getString(Constant.LINGHIT.linghit_id));
        allUserInfo.setFirst_time(object.getString(Constant.LINGHIT.first_time));
        allUserInfo.setApp(object.getString(Constant.LINGHIT.app));
        allUserInfo.set$mail(object.getString(Constant.LINGHIT.$mail));
        allUserInfo.setFirst_use_time(object.getString(Constant.LINGHIT.first_use_time));
        allUserInfo.setFirst_regist_time(object.getString(Constant.LINGHIT.first_regist_time));
        allUserInfo.setFirst_login_time(object.getString(Constant.LINGHIT.first_login_time));
        allUserInfo.setFirst_consume_time(object.getString(Constant.LINGHIT.first_consume_time));
        allUserInfo.set$channel_name(object.getString(Constant.LINGHIT.$channel_name));
        allUserInfo.set$channel_group(object.getString(Constant.LINGHIT.$channel_group));
        allUserInfo.set$channel_group_name(object.getString(Constant.LINGHIT.$channel_group_name));
        allUserInfo.set$app_userid(object.getString(Constant.LINGHIT.$app_userid));
        allUserInfo.set$token_person(object.getString(Constant.LINGHIT.$token_person));
        allUserInfo.set$token_umeng(object.getString(Constant.LINGHIT.$token_umeng));
        allUserInfo.set$username(object.getString(Constant.LINGHIT.$username));
        allUserInfo.set$alias(object.getString(Constant.LINGHIT.$alias));
        allUserInfo.set$birthday(object.getString(Constant.LINGHIT.$birthday));
        allUserInfo.set$age(object.getString(Constant.LINGHIT.$age));
        allUserInfo.set$gender(object.getString(Constant.LINGHIT.$gender));
        allUserInfo.set$qq(object.getString(Constant.LINGHIT.$qq));
        allUserInfo.set$wechat(object.getString(Constant.LINGHIT.$wechat));
        allUserInfo.set$work_status(object.getString(Constant.LINGHIT.$work_status));
        allUserInfo.set$profession(object.getString(Constant.LINGHIT.$profession));
        allUserInfo.set$marital_status(object.getString(Constant.LINGHIT.$marital_status));
        allUserInfo.set$equipment_brand(object.getString(Constant.LINGHIT.$equipment_brand));
        allUserInfo.set$equipment_brand_name(object.getString(Constant.LINGHIT.$equipment_brand_name));
        allUserInfo.set$equipment_code(object.getString(Constant.LINGHIT.$equipment_code));
        allUserInfo.set$sys(object.getString(Constant.LINGHIT.$sys));
        allUserInfo.set$sys_version(object.getString(Constant.LINGHIT.$sys_version));
        allUserInfo.set$browser(object.getString(Constant.LINGHIT.$browser));
        allUserInfo.set$network(object.getString(Constant.LINGHIT.$network));
        allUserInfo.set$wifi_switch(object.getString(Constant.LINGHIT.$wifi_switch));
        allUserInfo.set$phone_operator(object.getString(Constant.LINGHIT.$phone_operator));
        allUserInfo.set$phone_operator_name(object.getString(Constant.LINGHIT.$phone_operator_name));
        allUserInfo.set$token_phone(object.getString(Constant.LINGHIT.$token_phone));
        allUserInfo.set$push_brand(object.getString(Constant.LINGHIT.$push_brand));

        return allUserInfo;
    }

    public static Map<String, Object> getAllUserInfoMap(JSONObject jsonObject) {

        AllUserInfo allUserInfo = LinghitUtil.getAllUserInfo(jsonObject);
        Map<String, Object> allUserInfoMap = new HashMap<>();

        try{
            allUserInfoMap.put(Constant.LINGHIT.linghit_id, allUserInfo.getLinghit_id());
            allUserInfoMap.put(Constant.LINGHIT.first_time, allUserInfo.getFirst_time());
            allUserInfoMap.put(Constant.LINGHIT.app, allUserInfo.getApp());
            allUserInfoMap.put(Constant.LINGHIT.app_id, allUserInfo.getApp_id());
            allUserInfoMap.put(Constant.LINGHIT.$cookie, allUserInfo.get$cookie());
            allUserInfoMap.put(Constant.LINGHIT.$openid, allUserInfo.get$openid());
            allUserInfoMap.put(Constant.LINGHIT.$equipment_id, allUserInfo.get$equipment_id());
            allUserInfoMap.put(Constant.LINGHIT.$phone, allUserInfo.get$phone());
            allUserInfoMap.put(Constant.LINGHIT.$mail, allUserInfo.get$mail());
            allUserInfoMap.put(Constant.LINGHIT.first_use_time, allUserInfo.getFirst_use_time());
            allUserInfoMap.put(Constant.LINGHIT.first_regist_time, allUserInfo.getFirst_regist_time());
            allUserInfoMap.put(Constant.LINGHIT.first_login_time, allUserInfo.getFirst_login_time());
            allUserInfoMap.put(Constant.LINGHIT.first_consume_time, allUserInfo.getFirst_consume_time());
            allUserInfoMap.put(Constant.LINGHIT.$app_version, allUserInfo.get$app_version());
            allUserInfoMap.put(Constant.LINGHIT.$channel, allUserInfo.get$channel());
            allUserInfoMap.put(Constant.LINGHIT.$channel_name, allUserInfo.get$channel_name());
            allUserInfoMap.put(Constant.LINGHIT.$channel_group, allUserInfo.get$channel_group());
            allUserInfoMap.put(Constant.LINGHIT.$channel_group_name, allUserInfo.get$channel_group_name());
            allUserInfoMap.put(Constant.LINGHIT.$app_userid, allUserInfo.get$app_userid());
            allUserInfoMap.put(Constant.LINGHIT.$token_person, allUserInfo.get$token_person());
            allUserInfoMap.put(Constant.LINGHIT.$token_umeng, allUserInfo.get$token_umeng());
            allUserInfoMap.put(Constant.LINGHIT.$username, allUserInfo.get$username());
            allUserInfoMap.put(Constant.LINGHIT.$alias, allUserInfo.get$alias());
            allUserInfoMap.put(Constant.LINGHIT.$birthday, allUserInfo.get$birthday());
            allUserInfoMap.put(Constant.LINGHIT.$age, allUserInfo.get$age());
            allUserInfoMap.put(Constant.LINGHIT.$gender, allUserInfo.get$gender());
            allUserInfoMap.put(Constant.LINGHIT.$ip, allUserInfo.get$ip());
            allUserInfoMap.put(Constant.LINGHIT.$city, allUserInfo.get$city());
            allUserInfoMap.put(Constant.LINGHIT.$province, allUserInfo.get$province());
            allUserInfoMap.put(Constant.LINGHIT.$country, allUserInfo.get$country());
            allUserInfoMap.put(Constant.LINGHIT.$qq, allUserInfo.get$qq());
            allUserInfoMap.put(Constant.LINGHIT.$wechat, allUserInfo.get$wechat());
            allUserInfoMap.put(Constant.LINGHIT.$work_status, allUserInfo.get$work_status());
            allUserInfoMap.put(Constant.LINGHIT.$profession, allUserInfo.get$profession());
            allUserInfoMap.put(Constant.LINGHIT.$marital_status, allUserInfo.get$marital_status());
            allUserInfoMap.put(Constant.LINGHIT.$equipment_brand, allUserInfo.get$equipment_brand());
            allUserInfoMap.put(Constant.LINGHIT.$equipment_brand_name, allUserInfo.get$equipment_brand_name());
            allUserInfoMap.put(Constant.LINGHIT.$equipment_code, allUserInfo.get$equipment_code());
            allUserInfoMap.put(Constant.LINGHIT.$sys, allUserInfo.get$sys());
            allUserInfoMap.put(Constant.LINGHIT.$sys_version, allUserInfo.get$sys_version());
            allUserInfoMap.put(Constant.LINGHIT.$browser, allUserInfo.get$browser());
            allUserInfoMap.put(Constant.LINGHIT.$screen_width, allUserInfo.get$screen_width());
            allUserInfoMap.put(Constant.LINGHIT.$screen_height, allUserInfo.get$screen_height());
            allUserInfoMap.put(Constant.LINGHIT.$screen_size, allUserInfo.get$screen_size());
            allUserInfoMap.put(Constant.LINGHIT.$network, allUserInfo.get$network());
            allUserInfoMap.put(Constant.LINGHIT.$wifi_switch, allUserInfo.get$wifi_switch());
            allUserInfoMap.put(Constant.LINGHIT.$phone_operator, allUserInfo.get$phone_operator());
            allUserInfoMap.put(Constant.LINGHIT.$phone_operator_name, allUserInfo.get$phone_operator_name());
            allUserInfoMap.put(Constant.LINGHIT.$token_phone, allUserInfo.get$token_phone());
            allUserInfoMap.put(Constant.LINGHIT.$push_brand, allUserInfo.get$push_brand());
        } catch (Exception e){
            e.printStackTrace();
        }

        return allUserInfoMap;
    }


    /**
     * 获取SubUserInfo Domain
     */
    public static SubUserInfo getSubUserInfo(JSONObject object) {
        SubUserInfo subUserInfo = new SubUserInfo();
        JSONObject attrObject = object.getJSONObject(Constant.LINGHIT.attr);

        subUserInfo.setLinghit_id(object.getString(Constant.LINGHIT.linghit_id));
        subUserInfo.setFirst_time(object.getString(Constant.LINGHIT.first_time));
        subUserInfo.setApp(object.getString(Constant.LINGHIT.app));
        subUserInfo.setApp_id(object.getString(Constant.LINGHIT.app_id));
        subUserInfo.setCookie(attrObject.getString(Constant.LINGHIT.cookie));
        subUserInfo.setOpenid(attrObject.getString(Constant.LINGHIT.openid));
        subUserInfo.set$equipment_id(attrObject.getString(Constant.LINGHIT.$equipment_id));
        subUserInfo.set$phone(attrObject.getString(Constant.LINGHIT.$phone));
        subUserInfo.set$mail(object.getString(Constant.LINGHIT.$mail));
        subUserInfo.setFirst_use_time(object.getString(Constant.LINGHIT.first_use_time));
        subUserInfo.setFirst_regist_time(object.getString(Constant.LINGHIT.first_regist_time));
        subUserInfo.setFirst_login_time(object.getString(Constant.LINGHIT.first_login_time));
        subUserInfo.setFirst_consume_time(object.getString(Constant.LINGHIT.first_consume_time));
        subUserInfo.set$app_version(attrObject.getString(Constant.LINGHIT.$app_version));
        subUserInfo.set$channel(attrObject.getString(Constant.LINGHIT.$channel));
        subUserInfo.set$channel_name(object.getString(Constant.LINGHIT.$channel_name));
        subUserInfo.set$channel_group(object.getString(Constant.LINGHIT.$channel_group));
        subUserInfo.set$channel_group_name(object.getString(Constant.LINGHIT.$channel_group_name));
        subUserInfo.set$app_userid(object.getString(Constant.LINGHIT.$app_userid));
        subUserInfo.set$token_person(object.getString(Constant.LINGHIT.$token_person));
        subUserInfo.set$token_umeng(object.getString(Constant.LINGHIT.$token_umeng));
        subUserInfo.set$username(object.getString(Constant.LINGHIT.$username));
        subUserInfo.set$alias(object.getString(Constant.LINGHIT.$alias));
        subUserInfo.set$birthday(object.getString(Constant.LINGHIT.$birthday));
        subUserInfo.set$age(object.getString(Constant.LINGHIT.$age));
        subUserInfo.set$gender(object.getString(Constant.LINGHIT.$gender));
        subUserInfo.set$ip(attrObject.getString(Constant.LINGHIT.$ip));
        subUserInfo.set$city(attrObject.getString(Constant.LINGHIT.$city));
        subUserInfo.set$province(attrObject.getString(Constant.LINGHIT.$province));
        subUserInfo.set$country(attrObject.getString(Constant.LINGHIT.$country));
        subUserInfo.set$qq(object.getString(Constant.LINGHIT.$qq));
        subUserInfo.set$wechat(object.getString(Constant.LINGHIT.$wechat));
        subUserInfo.set$work_status(object.getString(Constant.LINGHIT.$work_status));
        subUserInfo.set$profession(object.getString(Constant.LINGHIT.$profession));
        subUserInfo.set$marital_status(object.getString(Constant.LINGHIT.$marital_status));
        subUserInfo.set$equipment_brand(object.getString(Constant.LINGHIT.$equipment_brand));
        subUserInfo.set$equipment_brand_name(object.getString(Constant.LINGHIT.$equipment_brand_name));
        subUserInfo.set$equipment_code(object.getString(Constant.LINGHIT.$equipment_code));
        subUserInfo.set$sys(attrObject.getString(Constant.LINGHIT.$sys));
        subUserInfo.set$sys_version(attrObject.getString(Constant.LINGHIT.$sys_version));
        subUserInfo.set$browser(object.getString(Constant.LINGHIT.$browser));
        subUserInfo.set$screen_width(attrObject.getString(Constant.LINGHIT.$screen_width));
        subUserInfo.set$screen_height(attrObject.getString(Constant.LINGHIT.$screen_height));
        subUserInfo.set$screen_size(attrObject.getString(Constant.LINGHIT.$screen_size));
        subUserInfo.set$network(object.getString(Constant.LINGHIT.$network));
        subUserInfo.set$wifi_switch(object.getString(Constant.LINGHIT.$wifi_switch));
        subUserInfo.set$phone_operator(object.getString(Constant.LINGHIT.$phone_operator));
        subUserInfo.set$phone_operator_name(object.getString(Constant.LINGHIT.$phone_operator_name));
        subUserInfo.set$token_phone(object.getString(Constant.LINGHIT.$token_phone));
        subUserInfo.set$push_brand(object.getString(Constant.LINGHIT.$push_brand));

        return subUserInfo;
    }

    public static Map<String, Object> getSubUserInfoMap(JSONObject jsonObject) {

        SubUserInfo subUserInfo = LinghitUtil.getSubUserInfo(jsonObject);
        Map<String, Object> subUserInfoMap = new HashMap<>();

        subUserInfoMap.put(Constant.LINGHIT.linghit_id, subUserInfo.getLinghit_id());
        subUserInfoMap.put(Constant.LINGHIT.first_time, subUserInfo.getFirst_time());
        subUserInfoMap.put(Constant.LINGHIT.app, subUserInfo.getApp());
        subUserInfoMap.put(Constant.LINGHIT.app_id, subUserInfo.getApp_id());
        subUserInfoMap.put(Constant.LINGHIT.cookie, subUserInfo.getCookie());
        subUserInfoMap.put(Constant.LINGHIT.openid, subUserInfo.getOpenid());
        subUserInfoMap.put(Constant.LINGHIT.$equipment_id, subUserInfo.get$equipment_id());
        subUserInfoMap.put(Constant.LINGHIT.$phone, subUserInfo.get$phone());
        subUserInfoMap.put(Constant.LINGHIT.$mail, subUserInfo.get$mail());
        subUserInfoMap.put(Constant.LINGHIT.first_use_time, subUserInfo.getFirst_use_time());
        subUserInfoMap.put(Constant.LINGHIT.first_regist_time, subUserInfo.getFirst_regist_time());
        subUserInfoMap.put(Constant.LINGHIT.first_login_time, subUserInfo.getFirst_login_time());
        subUserInfoMap.put(Constant.LINGHIT.first_consume_time, subUserInfo.getFirst_consume_time());
        subUserInfoMap.put(Constant.LINGHIT.$app_version, subUserInfo.get$app_version());
        subUserInfoMap.put(Constant.LINGHIT.$channel, subUserInfo.get$channel());
        subUserInfoMap.put(Constant.LINGHIT.$channel_name, subUserInfo.get$channel_name());
        subUserInfoMap.put(Constant.LINGHIT.$channel_group, subUserInfo.get$channel_group());
        subUserInfoMap.put(Constant.LINGHIT.$channel_group_name, subUserInfo.get$channel_group_name());
        subUserInfoMap.put(Constant.LINGHIT.$app_userid, subUserInfo.get$app_userid());
        subUserInfoMap.put(Constant.LINGHIT.$token_person, subUserInfo.get$token_person());
        subUserInfoMap.put(Constant.LINGHIT.$token_umeng, subUserInfo.get$token_umeng());
        subUserInfoMap.put(Constant.LINGHIT.$username, subUserInfo.get$username());
        subUserInfoMap.put(Constant.LINGHIT.$alias, subUserInfo.get$alias());
        subUserInfoMap.put(Constant.LINGHIT.$birthday, subUserInfo.get$birthday());
        subUserInfoMap.put(Constant.LINGHIT.$age, subUserInfo.get$age());
        subUserInfoMap.put(Constant.LINGHIT.$gender, subUserInfo.get$gender());
        subUserInfoMap.put(Constant.LINGHIT.$ip, subUserInfo.get$ip());
        subUserInfoMap.put(Constant.LINGHIT.$city, subUserInfo.get$city());
        subUserInfoMap.put(Constant.LINGHIT.$province, subUserInfo.get$province());
        subUserInfoMap.put(Constant.LINGHIT.$country, subUserInfo.get$country());
        subUserInfoMap.put(Constant.LINGHIT.$qq, subUserInfo.get$qq());
        subUserInfoMap.put(Constant.LINGHIT.$wechat, subUserInfo.get$wechat());
        subUserInfoMap.put(Constant.LINGHIT.$work_status, subUserInfo.get$work_status());
        subUserInfoMap.put(Constant.LINGHIT.$profession, subUserInfo.get$profession());
        subUserInfoMap.put(Constant.LINGHIT.$marital_status, subUserInfo.get$marital_status());
        subUserInfoMap.put(Constant.LINGHIT.$equipment_brand, subUserInfo.get$equipment_brand());
        subUserInfoMap.put(Constant.LINGHIT.$equipment_brand_name, subUserInfo.get$equipment_brand_name());
        subUserInfoMap.put(Constant.LINGHIT.$equipment_code, subUserInfo.get$equipment_code());
        subUserInfoMap.put(Constant.LINGHIT.$sys, subUserInfo.get$sys());
        subUserInfoMap.put(Constant.LINGHIT.$sys_version, subUserInfo.get$sys_version());
        subUserInfoMap.put(Constant.LINGHIT.$browser, subUserInfo.get$browser());
        subUserInfoMap.put(Constant.LINGHIT.$screen_width, subUserInfo.get$screen_width());
        subUserInfoMap.put(Constant.LINGHIT.$screen_height, subUserInfo.get$screen_height());
        subUserInfoMap.put(Constant.LINGHIT.$screen_size, subUserInfo.get$screen_size());
        subUserInfoMap.put(Constant.LINGHIT.$network, subUserInfo.get$network());
        subUserInfoMap.put(Constant.LINGHIT.$wifi_switch, subUserInfo.get$wifi_switch());
        subUserInfoMap.put(Constant.LINGHIT.$phone_operator, subUserInfo.get$phone_operator());
        subUserInfoMap.put(Constant.LINGHIT.$phone_operator_name, subUserInfo.get$phone_operator_name());
        subUserInfoMap.put(Constant.LINGHIT.$token_phone, subUserInfo.get$token_phone());
        subUserInfoMap.put(Constant.LINGHIT.$push_brand, subUserInfo.get$push_brand());

        return subUserInfoMap;
    }


    public static Map<String, Object> getUserEventsMap(JSONObject jsonObject) {

        UserEvents userEvents = LinghitUtil.getUserEvents(jsonObject);

        Map<String, Object> userEventsMap = new HashMap<>();
//         For Test
//        userEventsMap.put(Constant.LINGHIT.log_id, "056b1fa6");
////        long log_time = 1578499205L;
//        int log_time = 1578499205;
//        userEventsMap.put(Constant.LINGHIT.log_time, log_time);
//        userEventsMap.put(Constant.LINGHIT.app_id, "X01");
//        userEventsMap.put(Constant.LINGHIT.app, "88888888");
//        userEventsMap.put(Constant.LINGHIT.linghit_id, "12345678");

        userEventsMap.put(Constant.LINGHIT.log_id, userEvents.getLog_id());
        userEventsMap.put(Constant.LINGHIT.log_time, Integer.parseInt(userEvents.getLog_time()));
        userEventsMap.put(Constant.LINGHIT.app_id, userEvents.getApp_id());
        userEventsMap.put(Constant.LINGHIT.app, userEvents.getApp());
        userEventsMap.put(Constant.LINGHIT.linghit_id, userEvents.getLinghit_id());
        userEventsMap.put(Constant.LINGHIT.$app_version, userEvents.get$app_version());
        userEventsMap.put(Constant.LINGHIT.log_type, userEvents.getLog_type());
        userEventsMap.put(Constant.LINGHIT.user_id, userEvents.getUser_id());
        userEventsMap.put(Constant.LINGHIT.original_id, userEvents.getOriginal_id());
        userEventsMap.put(Constant.LINGHIT.event_id, userEvents.getEvent_id());
        userEventsMap.put(Constant.LINGHIT.event_name, userEvents.getEvent_name());
        userEventsMap.put(Constant.LINGHIT.$channel, userEvents.get$channel());
        userEventsMap.put(Constant.LINGHIT.$channel_name, userEvents.get$channel_name());
        userEventsMap.put(Constant.LINGHIT.$channel_group, userEvents.get$channel_group());
        userEventsMap.put(Constant.LINGHIT.$channel_group_name, userEvents.get$channel_group_name());
        userEventsMap.put(Constant.LINGHIT.$ip, userEvents.get$ip());
        userEventsMap.put(Constant.LINGHIT.$city, userEvents.get$city());
        userEventsMap.put(Constant.LINGHIT.$province, userEvents.get$province());
        userEventsMap.put(Constant.LINGHIT.$country, userEvents.get$country());
        userEventsMap.put(Constant.LINGHIT.$useragent, userEvents.get$useragent());
        userEventsMap.put(Constant.LINGHIT.$referrer, userEvents.get$referrer());
        userEventsMap.put(Constant.LINGHIT.$equipment_brand, userEvents.get$equipment_brand());
        userEventsMap.put(Constant.LINGHIT.$equipment_brand_name, userEvents.get$equipment_brand_name());
        userEventsMap.put(Constant.LINGHIT.$equipment_code, userEvents.get$equipment_code());
        userEventsMap.put(Constant.LINGHIT.$sys, userEvents.get$sys());
        userEventsMap.put(Constant.LINGHIT.$sys_version, userEvents.get$sys_version());
        userEventsMap.put(Constant.LINGHIT.$browser, userEvents.get$browser());
        userEventsMap.put(Constant.LINGHIT.$network, userEvents.get$network());
        userEventsMap.put(Constant.LINGHIT.$phone_operator, userEvents.get$phone_operator());
        userEventsMap.put(Constant.LINGHIT.$phone_operator_name, userEvents.get$phone_operator_name());
        userEventsMap.put(Constant.LINGHIT.$regist_way, userEvents.get$regist_way());
        userEventsMap.put(Constant.LINGHIT.$login_way, userEvents.get$login_way());
        userEventsMap.put(Constant.LINGHIT.$url, userEvents.get$url());
        userEventsMap.put(Constant.LINGHIT.$title, userEvents.get$title());
        userEventsMap.put(Constant.LINGHIT.$scan_time, userEvents.get$scan_time());
        userEventsMap.put(Constant.LINGHIT.$module, userEvents.get$module());
        userEventsMap.put(Constant.LINGHIT.$start_way, userEvents.get$start_way());
        userEventsMap.put(Constant.LINGHIT.$order_id, userEvents.get$order_id());
        userEventsMap.put(Constant.LINGHIT.$order_price, userEvents.get$order_price());
        userEventsMap.put(Constant.LINGHIT.$goods_name, userEvents.get$goods_name());
        userEventsMap.put(Constant.LINGHIT.$goods_price, userEvents.get$goods_price());
        userEventsMap.put(Constant.LINGHIT.$goods_amount, userEvents.get$goods_amount());
        userEventsMap.put(Constant.LINGHIT.$pay_way, userEvents.get$pay_way());
        userEventsMap.put(Constant.LINGHIT.$pay_price, userEvents.get$pay_price());
        userEventsMap.put(Constant.LINGHIT.$pay_result, userEvents.get$pay_result());
        userEventsMap.put(Constant.LINGHIT.ship, userEvents.getShip());
        userEventsMap.put(Constant.LINGHIT.discount, userEvents.getDiscount());
        userEventsMap.put(Constant.LINGHIT.address, userEvents.getAddress());
        userEventsMap.put(Constant.LINGHIT.$star_id, userEvents.get$star_id());
        userEventsMap.put(Constant.LINGHIT.row_date, userEvents.getRow_date());

        return userEventsMap;
    }


    /**
     * 获取UserEvents Domain
     */
    public static UserEvents getUserEvents(JSONObject object) {
        UserEvents userEvents = new UserEvents();
        JSONObject attrObject = object.getJSONObject(Constant.LINGHIT.attr);

        userEvents.setLog_id(object.getString(Constant.LINGHIT.log_id));
        userEvents.setLog_time(object.getString(Constant.LINGHIT.log_time));
        userEvents.setApp_id(object.getString(Constant.LINGHIT.app_id));
        userEvents.setLog_type(object.getString(Constant.LINGHIT.log_type));
        userEvents.setUser_id(object.getString(Constant.LINGHIT.user_id));
        userEvents.setOriginal_id(object.getString(Constant.LINGHIT.original_id));
        userEvents.setEvent_id(object.getString(Constant.LINGHIT.event_id));
        userEvents.set$app_version(attrObject.getString(Constant.LINGHIT.$app_version));
        userEvents.set$channel(attrObject.getString(Constant.LINGHIT.$channel));
        userEvents.set$ip(attrObject.getString(Constant.LINGHIT.$ip));
        userEvents.set$city(attrObject.getString(Constant.LINGHIT.$city));
        userEvents.set$province(attrObject.getString(Constant.LINGHIT.$province));
        userEvents.set$country(attrObject.getString(Constant.LINGHIT.$country));
        userEvents.set$useragent(attrObject.getString(Constant.LINGHIT.$useragent));
        userEvents.set$referrer(attrObject.getString(Constant.LINGHIT.$referrer));
        userEvents.set$sys(attrObject.getString(Constant.LINGHIT.$sys));
        userEvents.set$sys_version(attrObject.getString(Constant.LINGHIT.$sys_version));
        userEvents.set$url(attrObject.getString(Constant.LINGHIT.$url));
        userEvents.set$title(attrObject.getString(Constant.LINGHIT.$title));
        userEvents.set$scan_time(attrObject.getString(Constant.LINGHIT.$scan_time));

        String app = object.getString(Constant.LINGHIT.app);
        userEvents.setApp(app);
//        userEvents.setApp(app==null?"":app);
        String linghit_id = object.getString(Constant.LINGHIT.linghit_id);
        userEvents.setLinghit_id(linghit_id==null?"":linghit_id);
        String event_name = object.getString(Constant.LINGHIT.event_name);
        userEvents.setEvent_name(event_name==null?"":event_name);
        String $channel_name = object.getString(Constant.LINGHIT.$channel_name);
        userEvents.set$channel_name($channel_name==null?"":$channel_name);
        String $channel_group = object.getString(Constant.LINGHIT.$channel_group);
        userEvents.set$channel_group($channel_group==null?"":$channel_group);
        String $channel_group_name = object.getString(Constant.LINGHIT.$channel_group_name);
        userEvents.set$channel_group_name($channel_group_name==null?"":$channel_group_name);
        String $equipment_brand = object.getString(Constant.LINGHIT.$equipment_brand);
        userEvents.set$equipment_brand($equipment_brand==null?"":$equipment_brand);
        String $equipment_brand_name = object.getString(Constant.LINGHIT.$equipment_brand_name);
        userEvents.set$equipment_brand_name($equipment_brand_name==null?"":$equipment_brand_name);
        String $equipment_code = object.getString(Constant.LINGHIT.$equipment_code);
        userEvents.set$equipment_code($equipment_code==null?"":$equipment_code);
        String $browser = object.getString(Constant.LINGHIT.$browser);
        userEvents.set$browser($browser==null?"":$browser);
        String $network = object.getString(Constant.LINGHIT.$network);
        userEvents.set$network($network==null?"":$network);
        String $phone_operator = object.getString(Constant.LINGHIT.$phone_operator);
        userEvents.set$phone_operator($phone_operator==null?"":$phone_operator);
        String $phone_operator_name = object.getString(Constant.LINGHIT.$phone_operator_name);
        userEvents.set$phone_operator_name($phone_operator_name==null?"":$phone_operator_name);
        String $regist_way = object.getString(Constant.LINGHIT.$regist_way);
        userEvents.set$regist_way($regist_way==null?"":$regist_way);
        String $login_way = object.getString(Constant.LINGHIT.$login_way);
        userEvents.set$login_way($login_way==null?"":$login_way);
        String $module = object.getString(Constant.LINGHIT.$module);
        userEvents.set$module($module==null?"":$module);
        String $start_way = object.getString(Constant.LINGHIT.$start_way);
        userEvents.set$start_way($start_way==null?"":$start_way);
        String $order_id = object.getString(Constant.LINGHIT.$order_id);
        userEvents.set$order_id($order_id==null?"":$order_id);
//        String $order_price = object.getString(Constant.LINGHIT.$order_price);
//        userEvents.set$order_price($order_price==null?"":$order_price);
        String $goods_name = object.getString(Constant.LINGHIT.$goods_name);
        userEvents.set$goods_name($goods_name==null?"":$goods_name);
//        String $goods_price = object.getString(Constant.LINGHIT.$goods_price);
//        userEvents.set$goods_price($goods_price==null?"":$goods_price);
        String $goods_amount = object.getString(Constant.LINGHIT.$goods_amount);
        userEvents.set$goods_amount($goods_amount==null?"":$goods_amount);
        String $pay_way = object.getString(Constant.LINGHIT.$pay_way);
        userEvents.set$pay_way($pay_way==null?"":$pay_way);
//        String $pay_price = object.getString(Constant.LINGHIT.$pay_price);
//        userEvents.set$pay_price($pay_price==null?"":$pay_price);
//        String $pay_result = object.getString(Constant.LINGHIT.$pay_result);
//        userEvents.set$pay_result($pay_result==null?"":$pay_result);
        String ship = object.getString(Constant.LINGHIT.ship);
        userEvents.setShip(ship==null?"":ship);
        String discount = object.getString(Constant.LINGHIT.discount);
        userEvents.setDiscount(discount==null?"":discount);
        String address = object.getString(Constant.LINGHIT.address);
        userEvents.setAddress(address==null?"":address);
        String $star_id = object.getString(Constant.LINGHIT.$star_id);
        userEvents.set$star_id($star_id==null?"":$star_id);
        String row_date = object.getString(Constant.LINGHIT.row_date);
        userEvents.setRow_date(row_date==null?"":row_date);

        return userEvents;
    }

    /**
     * 生成大数据唯一ID
     */
    public static String generateLinghitId() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    /**
     * 获取灵机用户,cookie、openid、$equipment_id，$phone放在attr中
     */
    public static LinghitUser getUser(JSONObject object) {
        LinghitUser user = new LinghitUser();
        user.setUserid_type(object.getString(Constant.LINGHIT.target_type));
        JSONObject attrObject = object.getJSONObject(Constant.LINGHIT.attr);
        user.setUser(attrObject.getString(user.getUserid_type()));
        user.setPhone(attrObject.getString(Constant.LINGHIT.$phone));
        user.setOpenid(attrObject.getString(Constant.LINGHIT.openid));
        user.setEquipment_id(attrObject.getString(Constant.LINGHIT.$equipment_id));
        user.setCookie(attrObject.getString(Constant.LINGHIT.cookie));

        return user;
    }

    /**
     * 业务线字典：首次进入时间+业务线
     */
    public static String getProductMap(String log_time, String app_id) {
        return DateUtil.getFullDateFromTimeStamp(log_time) + "->" + app_id;
    }

    /**
     * 业务线字典的排序:"2019-12-07 15:25:49->X161,2019-12-08 15:25:49->X11,2019-12-08 15:26:49->X16";
     */
    public static String productMapSort(String first, String second) {

        if (TextUtils.isEmpty(first)) return second;
        if (TextUtils.isEmpty(second)) return first;

        try {
            String[] first_array = first.split(",");
            String[] second_array = second.split(",");

            List<ProductMap> mapList = new ArrayList<>();

            for (String single : first_array) {
                ProductMap product = new ProductMap();
                String[] singleArray = single.split("->");
                product.setTime(DateUtil.date2TimeStamp(singleArray[0]));
                product.setApp_id(singleArray[1]);
                product.setContent(single);
                mapList.add(product);
            }

            for (String single : second_array) {
                ProductMap product = new ProductMap();
                String[] singleArray = single.split("->");
                product.setTime(DateUtil.date2TimeStamp(singleArray[0]));
                product.setApp_id(singleArray[1]);
                product.setContent(single);
                mapList.add(product);
            }

            mapList.sort((o1, o2) -> {
                long time1 = o1.getTime();
                long time2 = o2.getTime();
                return Long.compare(time1, time2);
            });

            List<String> appid_list = new ArrayList<>();
            StringBuilder builder = new StringBuilder();
            for (ProductMap productMap : mapList) {
                if (!appid_list.contains(productMap.getApp_id())) {
                    builder.append(productMap.getContent());
                    builder.append(",");
                    appid_list.add(productMap.getApp_id());
                }
            }
            builder.deleteCharAt(builder.length() - 1);

            return builder.toString();
        } catch (Exception ignored) {
        }

        return second;
    }

    /**
     * 获取日志中的值
     */
    public static Map<String, String> getLogEventValue(JSONObject jsonObject) {
        Map<String, String> valueMap = new HashMap<>();

        JSONObject attrObject = jsonObject.getJSONObject(Constant.LINGHIT.attr);
        JSONObject sdkObject = jsonObject.getJSONObject(Constant.LINGHIT.sdk);
        String log_time = jsonObject.getString(Constant.LINGHIT.log_time);
        String app_id = jsonObject.getString(Constant.LINGHIT.app_id);
        String app_prex = app_id.substring(0, 1);
        // 移除掉，后面就不需要添加到字段里头来
        jsonObject.remove(Constant.LINGHIT.attr);
        jsonObject.remove(Constant.LINGHIT.sdk);

        // 先解析公共参数，包括sdk字段中的内容
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            String key = entry.getKey();
            if (entry.getValue() == null || TextUtils.isEmpty(entry.getValue().toString())) continue;
            String value = entry.getValue().toString();
            if (Constant.LINGHIT.event_id.equals(key) || Constant.LINGHIT.event_name.equals(key)) {
                if (Constant.LINGHIT.event_id.equals(key)) {
                    valueMap.put(Constant.LINGHIT.event_id, value);
                    String event_name = EventUDF.evaluate(value);
//                System.out.println("测试:event_name:" + event_name);
                    if (!TextUtils.isEmpty(event_name)) {
                        valueMap.put(Constant.LINGHIT.event_name, event_name);
                    }
                }
            } else {
                valueMap.put(key, value);
            }
        }
        // 解析sdk对象中的参数
        for (Map.Entry<String, Object> entry : sdkObject.entrySet()) {
            Object value = entry.getValue();
            if (value != null && TextUtils.isNotEmpty(value.toString())) {
                valueMap.put(entry.getKey(), value.toString());
            }
        }
        // 解析attr对象中的自定义属性
        // java/php/android/ios/js ios和android属于app，不同日志来源解析方式有不同
        String sdk_id = sdkObject.getString(Constant.LINGHIT.sdk_id);
        boolean isApp = "ios".equals(sdk_id) || "android".equals(sdk_id);
        for (Map.Entry<String, Object> entry : attrObject.entrySet()) {
            Object value = entry.getValue();
            if (value != null && TextUtils.isNotEmpty(value.toString())) {
//                    long start1 = System.currentTimeMillis();
                resolveAttrField(valueMap, entry.getKey(), value.toString(), app_prex, isApp);
//                    System.out.println("测试:耗时解析:" + entry.getKey() + (System.currentTimeMillis() - start1));
            }
        }

        valueMap.put(Constant.LINGHIT.row_date, DateUtil.parseTime(log_time));
        valueMap.put(Constant.LINGHIT.app, AppUtil.getApp(app_id));

        return valueMap;
    }

    /**
     * 对自定义的字段进行解析
     *
     * @param app_prex 业务线前缀:比如在线A
     * @param isApp    java/php/android/ios/js ios和android属于app
     */
    private static void resolveAttrField(Map<String, String> valueMap, String key, String value, String app_prex, boolean isApp) {

        if (Constant.LINGHIT.$channel.equals(key)) {
            valueMap.put(Constant.LINGHIT.$channel, ChannelUDF.evaluate(value, 1, app_prex));
            String channel = ChannelUDF.evaluate(value, 2, app_prex);
            // $channel_group和$channel_group_name是一样的，没必要计算两次
            valueMap.put(Constant.LINGHIT.$channel_group, channel);
            valueMap.put(Constant.LINGHIT.$channel_group_name, channel);
        } else if (Constant.LINGHIT.$city.equals(key) || Constant.LINGHIT.$province.equals(key)) {
            valueMap.put(key, GeographyUDF.evaluate(value));
        } else if (Constant.LINGHIT.$order_price.equals(key) || Constant.LINGHIT.$pay_price.equals(key)) {
            Double price = PriceUDF.evaluate(value);
            if (price != null)
                valueMap.put(key, String.valueOf(price));
        } else if (Constant.LINGHIT.$useragent.equals(key)) {
            // $useragent只有web端时候进行处理，取出其中的设备信息
            if (isApp) {
                valueMap.put(key, value);
            } else {
                Map<String, String> map = UseragentUDF2.evaluate(value);
                String equipment_brand = map.get("equipment_brand");
                String equipment_brand_name = map.get("equipment_brand_name");
                valueMap.put(Constant.LINGHIT.$equipment_brand, EquipmentUDF.evaluate(equipment_brand, 1));
                valueMap.put(Constant.LINGHIT.$equipment_brand_name, EquipmentUDF.evaluate(equipment_brand_name, 2));
                valueMap.put(Constant.LINGHIT.$equipment_code, map.get("equipment_code"));
                valueMap.put(Constant.LINGHIT.$sys, map.get("sys"));
                valueMap.put(Constant.LINGHIT.$sys_version, map.get("sys_version"));
                valueMap.put(Constant.LINGHIT.$browser, map.get("browser"));
            }
        } else if (Constant.LINGHIT.$equipment_brand.equals(key) || Constant.LINGHIT.$equipment_brand_name.equals(key)) {
            // 这个只有app时候进行处理
            if (Constant.LINGHIT.$equipment_brand.equals(key) && isApp) {
                valueMap.put(key, EquipmentUDF.evaluate(value, 1));
                valueMap.put(Constant.LINGHIT.$equipment_brand_name, EquipmentUDF.evaluate(value, 2));
            }
        } else if (Constant.LINGHIT.$equipment_code.equals(key) || Constant.LINGHIT.$sys.equals(key) || Constant.LINGHIT.$sys_version.equals(key) || Constant.LINGHIT.$browser.equals(key)) {
            // 对于web端的这些信息要在$useragent中进行获取，app端就直接添加
            if (isApp) {
                valueMap.put(key, value);
            }
        } else if (Constant.LINGHIT.$birthday.equals(key) || Constant.LINGHIT.$age.equals(key)) {

            Long birthday = null;
            if (Constant.LINGHIT.$birthday.equals(key) && !isApp) { // web端
                birthday = BirthdayUDF.evaluate(value, 1);

            } else if (Constant.LINGHIT.$birthday.equals(key) && isApp) { // app端
                birthday = BirthdayUDF.evaluate(value, 2);
            }

            Integer age = AgeUDF.evaluate(birthday);
            if (birthday != null)
                valueMap.put(Constant.LINGHIT.$birthday, String.valueOf(birthday));
            if (age != null)
                valueMap.put(Constant.LINGHIT.$age, String.valueOf(age));
        } else if (Constant.LINGHIT.$gender.equals(key)) {
            valueMap.put(key, GenderUDF.evaluate(value));
        } else if (Constant.LINGHIT.$phone.equals(key)) {
            valueMap.put(key, PhoneUDF.evaluate(value));
        } else {
            valueMap.put(key, value);
        }
    }

}
