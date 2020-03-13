import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.linghit.constant.TableScheme;
import com.linghit.dao.RedisDao;
import com.linghit.util.ConfigUtil;
import redis.clients.jedis.Jedis;

public class RedisTest {

    public static void main(String[] atgs) throws Exception {
//        ConfigUtil.init();
//        System.out.println();
//        addUserInfoData();
//        addUserFlowData();
//        getData();
        System.out.println(TableScheme.zxcs_info_map);
    }

    private static void addUserFlowData() {

    }


    private static void addUserInfoData() {
        Jedis jedis = RedisDao.getJedis();
        JSONObject object = new JSONObject();
        object.put("type", "string");
        object.put("app_id", "string");
        object.put("$app_version", "string");
        object.put("$channel", "string");
        object.put("$channel_name", "string");
        object.put("$channel_group", "string");
        object.put("$channel_group_name", "string");
        object.put("user_id", "string");
        object.put("$app_userid", "string");
        object.put("$openid", "string");
        object.put("$token_person", "string");
        object.put("$token_umeng", "string");
        object.put("$username", "string");
        object.put("$alias", "string");
        object.put("$birthday", "string");
        object.put("$age", "int");
        object.put("$gender", "string");
        object.put("$ip", "string");
        object.put("$city", "string");
        object.put("$province", "string");
        object.put("$country", "string");
        object.put("$mail", "string");
        object.put("$phone", "string");
        object.put("$qq", "string");
        object.put("$wechat", "string");
        object.put("$work_status", "string");
        object.put("$profession", "string");
        object.put("$marital_status", "string");
        object.put("$equipment_brand", "string");
        object.put("$equipment_brand_name", "string");
        object.put("$equipment_code", "string");
        object.put("$sys", "string");
        object.put("$sys_version", "string");
        object.put("$browser", "string");
        object.put("$screen_width", "string");
        object.put("$screen_height", "string");
        object.put("$screen_size", "string");
        object.put("$network", "string");
        object.put("$wifi_switch", "string");
        object.put("$phone_operator", "string");
        object.put("$phone_operator_name", "string");
        object.put("regist_time", "long");
        object.put("start_time", "long");
        object.put("row_date", "string");
        object.put("schannel", "string");
        jedis.setex("A00-userinfo", 36000000, object.toJSONString());
    }

    private static void getData() {
        Jedis jedis = RedisDao.getJedis();
        String res_str = jedis.get("A00-userinfo");
        JSONObject object = JSON.parseObject(res_str);
        System.out.println(object.toJSONString());
    }

}
