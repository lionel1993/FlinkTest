package com.linghit.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.linghit.util.AppUtil;
import com.linghit.util.TextUtils;
import org.apache.kudu.Type;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

import static com.linghit.util.ConfigUtil.getProperties;

public class RedisDao {

    private static JedisPool pool;
    private static int database;
    private static Map<String, String> tableSchemeMap = new HashMap<>();

    public static JedisPool initOrGetPool() {

        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxIdle(50);// 最大空闲
            config.setMaxTotal(100);// 最大连接数
            System.out.println(getProperties("redis_port"));
            pool = new JedisPool(config,
                    getProperties("redis_ip"),
                    Integer.parseInt(getProperties("redis_port")),
                    Integer.parseInt(getProperties("redis_request_timeout")) * 1000,
                    getProperties("redis_passwd"));

            database = Integer.parseInt(getProperties("redis_dp_database"));
        }
        return pool;
    }

    public static Jedis getJedis() {
        Jedis jedis = initOrGetPool().getResource();
        jedis.select(database);
        return jedis;
    }

    /**
     * 获取kudu表的元数据信息,列名:类型
     */
    public static Map<String, Type> getTableColumnsForRedis(String app_id, boolean isUser) {

        Jedis jedis = null;
        Map<String, Type> kuduSchemeMap = new HashMap<>();

        try {
            String app = AppUtil.getApp(app_id);
            String tableType = isUser ? "userinfo" : "userevent";
            // 获取redis对应元数据信息
            jedis = RedisDao.getJedis();
            String key = app + "-" + tableType;
            String res_str = jedis.get(key);
//        System.out.println("测试:" + key + ":" + res_str);

            if (TextUtils.isEmpty(res_str)) {
                res_str = tableSchemeMap.get(key);
            } else {
                tableSchemeMap.put(key, res_str);
            }

            JSONObject object = JSON.parseObject(res_str);
            Map<String, Object> newMap = object.getInnerMap();

            for (Map.Entry<String, Object> entry : newMap.entrySet()) {
                kuduSchemeMap.put(entry.getKey(), getColumnType(String.valueOf(entry.getValue())));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) RedisDao.closeJedis(jedis);
        }

        return kuduSchemeMap;
    }

    private static Type getColumnType(String columnType) {
        Type type;
        columnType = columnType.trim();
        if ("int".equals(columnType)) {
            type = Type.INT8;
        } else if ("double".equals(columnType)) {
            type = Type.DOUBLE;
        } else if ("float".equals(columnType)) {
            type = Type.FLOAT;
        } else if ("string".equals(columnType)) {
            type = Type.STRING;
        } else if ("long".equals(columnType)) {
            type = Type.INT32;
        } else {
            type = Type.STRING;
        }
        return type;
    }

    private static String getColumnString(Type columnType) {
        String columnString;
        if (Type.INT8 == columnType) {
            columnString = "int";
        } else if (Type.DOUBLE == columnType) {
            columnString = "double";
        } else if (Type.FLOAT == columnType) {
            columnString = "float";
        } else if (Type.STRING == columnType) {
            columnString = "string";
        } else if (Type.INT32 == columnType) {
            columnString = "long";
        } else {
            columnString = "string";
        }
        return columnString;
    }

    public static void closeJedis(Jedis jedis) {
        try {
            initOrGetPool().returnResource(jedis);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //initOrGetPool().close();
    }
}
