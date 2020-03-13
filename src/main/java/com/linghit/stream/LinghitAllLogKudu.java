package com.linghit.stream;

import com.alibaba.fastjson.JSONObject;
import com.linghit.constant.Constant;
import com.linghit.dao.HbaseDao;
import com.linghit.dao.KuduDao;
import com.linghit.domain.LinghitUser;
import com.linghit.hbase.HbaseConnectionUtil;
import com.linghit.util.ConfigUtil;
import com.linghit.util.LinghitUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.serde.DefaultSerDe;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.kudu.client.*;

import java.text.SimpleDateFormat;
import java.util.*;


/**
 * 3.0用户体系日志处理
 */
public class LinghitAllLogKudu {

    public static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    public static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        //初始化配置
        ConfigUtil.init();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置启动检查点 checkpoint的间隔毫秒数
        env.enableCheckpointing(10000);

//        env.setParallelism(6); // 设置并行度

        //设置以事件事件为时间基准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
        构建配置类
         */
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", ConfigUtil.getProperties("zookeeper_host"));
        props.setProperty("bootstrap.servers", ConfigUtil.getProperties("kafka_host"));
        props.setProperty("group.id", ConfigUtil.getProperties("linghit_log_group"));
        props.setProperty("enable.auto.commit", "true");
        System.out.println("****配置设置成功****");

        /*
        构建kafka流
         */
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(ConfigUtil.getProperties("linghit_log_topic"),new SimpleStringSchema(),props);
        DataStream<String> stream = env.addSource(consumer);

        //打印日志
        stream.print().setParallelism(1);

        //构建表数组
        String[] tables = new String[]{
                "linghit_all_userevent_kudu",    //事件总表
                "linghit_all_userinfo",          //公司级别用户表
                "linghit_sub_userinfo",          //业务线级别用户表
                "linghit_all_userindex_cookie",  //用户索引表
                "linghit_all_userindex_openid",
                "linghit_all_userindex_equipment_id",
                "linghit_all_userindex_phone"
        };
        //获取sink
        /*
            注意：sink的输入是列表，支持多个顺序的操作，每个KuduRow是一个操作。
            目前之前upset和delete操作,默认KuduRow的操作是upset
         */
        KuduSink<List<KuduRow>> sink = KuduDao.getSink(tables);

        /*
        map操作，处理每一条日志
         */
        //stream.rebalance().map((MapFunction<String, List<KuduRow>>) value -> {        //lambda表达式不支持List的返回
        stream.rebalance().map( new MapFunction<String, List<KuduRow>>() {

            @Override
            public List<KuduRow> map(String value) throws Exception {

                JSONObject jsonObj = JSONObject.parseObject(value);

                return writeIntoKudu(jsonObj);

            }
        }).addSink(sink);

        try {
            env.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 处理每条日志写入kudu
     * @param jsonObj
     * @return
     */
    private static List<KuduRow> writeIntoKudu(JSONObject jsonObj) {
        Connection conn = HbaseConnectionUtil.getHbaseConnection();
        try {
            List<KuduRow> kuduRowList = new ArrayList<>();
            String log_id = jsonObj.getString("log_id");
            int log_time = jsonObj.getInteger("log_time");
            String app_id = jsonObj.getString("app_id");
            String event_id = jsonObj.getString("event_id");
            String log_type = jsonObj.getString("log_type");
            String city = jsonObj.getString("$city");

            if (log_id == null || log_id.equals("")) {
                log_id = UUID.randomUUID().toString().replaceAll("-", "");
            }
            if (app_id == null || app_id.equals("")) {
                System.out.println("app_id is null");
                return null;
            }
            String app = app_id.substring(0, 1);

            Date log_date = new Date();
            log_date.setTime(log_time * 1000L);
            String row_date = sdf.format(log_date);

            System.out.println("log_type:" + log_type + ",log_id:" + log_id + ",log_time:" + log_time
                    + "，app_id:" + app_id + "，app:" + app + "，event_id:" + event_id + "，row_date:" + row_date+ "，city:" + city);

            try {
                switch (log_type) {
                    case "user_info": {         //用户信息
                        //insertNewUser(conn, jsonObject);
                        break;
                    }
                    case "user_info_link": {         //用户关联
                        //linkUserAndDevice(conn, jsonObject);
                        break;
                    }
                    default: {                 //默认写入事件表
                        kuduRowList = insertUserEvent(conn, jsonObj);
                        break;
                    }
                }
            } catch ( Exception e) {
                e.printStackTrace();
            }

            return kuduRowList;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 生成用户信息
     * @param jsonObject
     * @return
     */
    public static  List<KuduRow> insertUserInfo(Connection conn, JSONObject jsonObject) throws Exception {
        List<KuduRow> kuduRowList = new ArrayList<>();

        // 1、取出用户标识
        LinghitUser user = LinghitUtil.getUser(jsonObject);
        // 2、取出日志事件中各个字段值
        Map<String, String> eventValueMap = LinghitUtil.getLogEventValue(jsonObject);
        // 避免采集方采集错乱，以user和userid_type为主
        eventValueMap.put(user.getUserid_type(), user.getUser());
        eventValueMap.put(Constant.LINGHIT.data_type, "log");

        String index_user_prex = null;
        if (Constant.LINGHIT.$phone.equals(user.getUserid_type())) {
            index_user_prex = Constant.LINGHIT.PREX_PHONE;
        } else if (Constant.LINGHIT.$equipment_id.equals(user.getUserid_type())) {
            index_user_prex = Constant.LINGHIT.PREX_EQUIPMENT_ID;
        } else if (Constant.LINGHIT.openid.equals(user.getUserid_type())) {
            index_user_prex = Constant.LINGHIT.PREX_OPENID;
        } else if (Constant.LINGHIT.cookie.equals(user.getUserid_type())) {
            index_user_prex = Constant.LINGHIT.PREX_COOKIE;
        }
        String index_user_key = index_user_prex + user.getUser();
        // 必须有一个用户标识
        if (StringUtils.isEmpty(index_user_prex) || StringUtils.isEmpty(user.getUser())) return null;

        // 通过user_id查询用户索引表获取linghit_id
        // 1、查询得到，直接使用linghit_id
        // 2、查询不到，通过user_id生成linghit_id，
        //   (1)索引表创建索引(rowkey=openid,equipment_id,$phone->linghit_id)
        //   (2)用户表创建一个新用户(rowkey=linghit_id)
        List<String> indexColumnList = new ArrayList<>();
        indexColumnList.add(Constant.LINGHIT.linghit_id);
        // 查询索引表
        TableName userIndexTableName = TableName.valueOf(""); // 用户索引表
        Table userIndexTable = conn.getTable(userIndexTableName);
        /*Map<String, String> indexDataMap = query(userIndexTable, index_user_key, indexColumnList);
        if (indexDataMap.isEmpty()) { // 查询不到
        }*/

       /* KuduRow row = new KuduRow(6);
        row.setField(0, "log_id", jsonObject.get("log_id"));
        row.setField(1, "log_time", jsonObject.get("log_time"));
        row.setField(2, "app_id", "");
        row.setField(3, "app", "");
        row.setField(4, "event_id", "");
        row.setField(5, "row_date", "");
        row.setTableName("linghit_all_userevent_kudu");
        kuduRowList.add(row);*/

        return kuduRowList;
    }


    /**
     * 生成事件信息
     * @param jsonObject
     * @return
     */
    public static  List<KuduRow> insertUserEvent(Connection conn, JSONObject jsonObject) throws Exception {

        List<KuduRow> kuduRowList = new ArrayList<>();

        KuduRow row = new KuduRow(6);
        row.setField(0, "log_id", jsonObject.get("log_id"));
        row.setField(1, "log_time", jsonObject.get("log_time"));
        row.setField(2, "app_id", "");
        row.setField(3, "app", "");
        row.setField(4, "event_id", "");
        row.setField(5, "row_date", "");
        row.setTableName("linghit_all_userevent_kudu");
        kuduRowList.add(row);

        return kuduRowList;
    }

}
