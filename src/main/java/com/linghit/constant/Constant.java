package com.linghit.constant;

/**
 * 保存常量统一
 */
public class Constant {

    /**
     * 融合平台相关字段
     */
    public static class LINGHIT {
        /**
         * 用户相关标识
         */
        public static String linghit_id = "linghit_id"; // 大数据生成的唯一id
        public static String log_id = "log_id"; // 日志采集端生成的唯一id
        public static String target_type = "target_type"; // 指定用户类型,一般有四种：cookie、openid、$equipment_id，$phone
        public static String target_id = "target_id"; // 用户关联事件，一般是手机号
        public static String original_id = "original_id";
        public static String link_type = "link_type";
        public static String cookie = "cookie";
        public static String openid = "openid";
        public static String $equipment_id = "$equipment_id";
        public static String $phone = "$phone";

        /**
         * 日志的规定字段
         */
        public static String attr = "attr";
        public static String sdk = "sdk";
        public static String log_time = "log_time";
        public static String app = "app";
        public static String app_id = "app_id";
        public static String sdk_id = "sdk_id";
        public static String log_type = "log_type";
        public static String log_version = "log_version";
        public static String row_key = "row_key";
        public static String row_date = "row_date";
        public static String record_date = "record_date";
        public static String event_id = "event_id";
        public static String event_name = "event_name";
        public static String data_type = "data_type";
        public static String $channel = "$channel";
        public static String schannel = "schannel";
        public static String $channel_group = "$channel_group";
        public static String $channel_group_name = "$channel_group_name";
        public static String $city = "$city";
        public static String $province = "$province";
        public static String $country = "$country";
        public static String $order_price = "$order_price";
        public static String $pay_price = "$pay_price";
        public static String $useragent = "$useragent";
        public static String $equipment_brand = "$equipment_brand";
        public static String $equipment_brand_name = "$equipment_brand_name";
        public static String $equipment_code = "$equipment_code";
        public static String $sys = "$sys";
        public static String $sys_version = "$sys_version";
        public static String $browser = "$browser";
        public static String $product_map = "$product_map";
        public static String $sku_good_id = "$sku_good_id";

        /**
         * 需要进行清洗的用户独有字段
         */
        public static String $age = "$age";
        public static String $gender = "$gender";
        public static String $birthday = "$birthday";
        public static String $regist_time = "$regist_time";
        public static String $source_channel = "$source_channel"; // 来源渠道

        /**
         * 索引前缀
         */
        public static String PREX_COOKIE = "cookie:";
        public static String PREX_OPENID = "openid:";
        public static String PREX_PHONE = "phone:";
        public static String PREX_EQUIPMENT_ID = "equipment_id:";

        public static String _source = "_source";
        /**
         * 新用户体系UserEvents字段
         */
        public static String $app_version = "$app_version";
        public static String user_id = "user_id";
        public static String $channel_name = "$channel_name";
        public static String $ip = "$ip";
        public static String $referrer = "$referrer";
        public static String $network = "$network";
        public static String $phone_operator = "$phone_operator";
        public static String $phone_operator_name = "$phone_operator_name";
        public static String $regist_way = "$regist_way";
        public static String $login_way = "$login_way";
        public static String $url = "$url";
        public static String $title = "$title";
        public static String $scan_time = "$scan_time";
        public static String $module = "$module";
        public static String $start_way = "$start_way";
        public static String $order_id = "$order_id";
        public static String $goods_name = "$goods_name";
        public static String $goods_price = "$goods_price";
        public static String $goods_amount = "$goods_amount";
        public static String $pay_way = "$pay_way";
        public static String $pay_result = "$pay_result";
        public static String ship = "ship";
        public static String discount = "discount";
        public static String address = "address";
        public static String $star_id = "$star_id";


        /**
         * 新用户体系表AllUserInfo字段
         */
        public static String first_time = "first_time";
        public static String $cookie = "$cookie";
        public static String $openid = "$openid";
        public static String $mail = "$mail";
        public static String first_use_time = "first_use_time";
        public static String first_regist_time = "first_regist_time";
        public static String first_login_time = "first_login_time";
        public static String first_consume_time = "first_consume_time";
        public static String $app_userid = "$app_userid";
        public static String $token_person = "$token_person";
        public static String $token_umeng = "$token_umeng";
        public static String $username = "$username";
        public static String $alias = "$alias";
        public static String $qq = "$qq";
        public static String $wechat = "$wechat";
        public static String $work_status = "$work_status";
        public static String $profession = "$profession";
        public static String $marital_status = "$marital_status";
        public static String $screen_width = "$screen_width";
        public static String $screen_height = "$screen_height";
        public static String $screen_size = "$screen_size";
        public static String $wifi_switch = "$wifi_switch";
        public static String $token_phone = "$token_phone";
        public static String $push_brand = "$push_brand";

    }

}