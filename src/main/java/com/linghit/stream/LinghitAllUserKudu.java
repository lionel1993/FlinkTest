package com.linghit.stream;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.linghit.constant.Constant;
import com.linghit.dao.HbaseDao;
import com.linghit.dao.KuduDao;
import com.linghit.domain.AllUserInfo;
import com.linghit.domain.UserEvents;
import com.linghit.hbase.HbaseConnectionUtil;
import com.linghit.util.AppUtil;
import com.linghit.util.ConfigUtil;
import com.linghit.util.DateUtil;
import com.linghit.util.LinghitUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.*;


/**
 * 新用户体系，日志处理
 * $phone->$equipment_id->openid->cookie
 */
public class LinghitAllUserKudu {

    /**
     * KUDU 用户全表，子业务线用户表
     */
    private final static String USER_TABLE = "linghit_all_userinfo_test";
    private final static String USER_SUB_TABLE = "linghit_sub_userinfo_test";
    private final static String USER_EVENT_TABLE = "linghit_all_userevent_kudu_test";
    /**
     * Hbase 索引表：用户标示 -> linghit_id
     */
    private final static String USER_INFO_INDEX_TABLE = "linghit_userinfo_index";
    /**
     * Hbase 索引表：linghit_id -> 用户标示
     */
    private final static String USER_INFO_INDEX_REVERSE_TABLE = "linghit_userinfo_index_reverse";

    private final static String HBASEFAMILYCOLUMN = "attr";


    public static void main(String[] args) throws Exception {

        ConfigUtil.init();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000); // 设置启动检查点 checkpoint的间隔毫秒数
//        env.setParallelism(6); // 设置并行度
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", ConfigUtil.getProperties("zookeeper_host"));
        props.setProperty("bootstrap.servers", ConfigUtil.getProperties("kafka_host"));
        props.setProperty("group.id", ConfigUtil.getProperties("linghit_log_group"));
        props.setProperty("enable.auto.commit", "true");
        System.out.println("****配置设置成功****");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(ConfigUtil.getProperties("linghit_log_topic"),new SimpleStringSchema(),props);
        DataStream<String> stream = env.addSource(consumer);

        // 构建Kudu可操作的表数组
        String[] tables = new String[]{
                USER_TABLE,          // 公司级别用户表
                USER_SUB_TABLE,          // 业务线级别用户表
                USER_EVENT_TABLE,           // 事件表
                "impala::dev_linghit_dw_dwd.linghit_all_userevent_test"
        };

      /*  获取 KUDU Sink
            注意：sink的输入是列表，支持多个顺序的操作，每个KuduRow是一个操作，kuduRowList是一系列KuduRow集合。
            目前之前upset和delete操作，默认KuduRow的操作是upset*/

        KuduSink<List<KuduRow>> kuduSink = KuduDao.getSink(tables);
        stream.rebalance().map( new MapFunction<String, List<KuduRow>>() {
            @Override
            public List<KuduRow> map(String value) throws Exception {
//                System.out.println(value);
                return writeIntoKudu(value);
            }
        }).addSink(kuduSink);//.print(); // Flink添加自定义sink
        /*stream.rebalance().map(new MapFunction<String, Object>() {
            public String map(String value)throws IOException {
                System.out.println("value:"+value);
                writeIntoKudu(value);
                return value;
            }

        }).print();*/

        try {
            env.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static List<KuduRow> writeIntoKudu(String content) {
        Connection conn = HbaseConnectionUtil.getHbaseConnection();
        String logtype = "";
        try {
            List<KuduRow> kuduRowList = new ArrayList<>();
            JSONObject jsonObject = JSONObject.parseObject(content);
            System.out.println(System.currentTimeMillis()+"\nReceive:"+content+"\nJson Format:"+jsonObject);

            JSONObject sourceObj = jsonObject.getJSONObject(Constant.LINGHIT._source);

            logtype = sourceObj.getString(Constant.LINGHIT.log_type);
            System.out.println("logtype:"+logtype);
            // 3.0版本日志，log_version为空说明是以前版本
            if (!sourceObj.containsKey(Constant.LINGHIT.log_version)) return null;
            String log_version = sourceObj.getString(Constant.LINGHIT.log_version);
            boolean isLargeOrEqual = AppUtil.isLargeEqualVersion(log_version, "3.0");
            if (!isLargeOrEqual) return null;

            switch (logtype) {
                case "user_info_link":{
                    System.out.println("Enter user info link!");
                    kuduRowList = dealUserInfoLink(conn, sourceObj);
                    break;
                }
                case "user_info": {
                    System.out.println("Enter user info!");
                    kuduRowList = dealUserInfo(conn, sourceObj);
                    break;
                }
                default: {
                    System.out.println("Enter user event!");
                    kuduRowList = dealUserEvent(conn, sourceObj);
                    break;
                }
            }
            return kuduRowList;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * user_info 处理逻辑
     * @param conn hbase连接对象
     * @param jsonObject 日志json
     */
    private static List<KuduRow> dealUserInfo (Connection conn, JSONObject jsonObject){
        List<KuduRow> kuduRowList = new ArrayList<>();
        // 获取用户标示
        String targetType = jsonObject.getString(Constant.LINGHIT.target_type);
        String app_id = jsonObject.getString(Constant.LINGHIT.app_id);
        JSONObject attrObject = jsonObject.getJSONObject("attr");
        String targetValue = attrObject.getString(targetType);
        String userKey = targetType+":"+targetValue;
        Map<String, String> queryUserMap = null;
        // 根据用户标示查索引表
        try {
            queryUserMap = HbaseDao.getByKey(conn, USER_INFO_INDEX_TABLE, userKey);
        }catch (Exception e){
            e.printStackTrace();
//            return null;
        }

        Map<String, Object> allUserInfoMap = LinghitUtil.getAllUserInfoMap(jsonObject);
        Map<String, Object> subUserInfoMap = LinghitUtil.getSubUserInfoMap(jsonObject);
        Map<String, Object> userEventsMap = LinghitUtil.getUserEventsMap(jsonObject);
        allUserInfoMap.put("app_id",app_id);
        subUserInfoMap.put("app_id",app_id);
        System.out.println("queryUserMap："+queryUserMap);

        String linghit_id;
        if (queryUserMap.isEmpty() || !queryUserMap.containsKey("linghit_id") || StringUtils.isEmpty(queryUserMap.get("linghit_id"))){  //没有找到userKey的用户
            System.out.println("索引表未查到用户");
            linghit_id = LinghitUtil.generateLinghitId();  //生成linghit_id
            // ADD 到索引表
            TableName userIndexName = TableName.valueOf(USER_INFO_INDEX_TABLE);
            HbaseDao.putRowColumnValue(conn, userIndexName, userKey, HBASEFAMILYCOLUMN, "linghit_id", linghit_id);
            // ADD 到反索引表
//            TableName userIndexReverseName = TableName.valueOf(USER_INFO_INDEX_REVERSE_TABLE);
//            HbaseDao.putRowColumnValue(conn, userIndexReverseName, linghit_id, HBASEFAMILYCOLUMN, targetType, targetValue);
            // ADD 到全司用户信息表
            allUserInfoMap.put("linghit_id", queryUserMap.get("linghit_id").toString());
            allUserInfoMap.put("first_time", 1577808003);
            KuduRow userInfoRow = new KuduRow(allUserInfoMap.size());
            int idx_userinfo = 0;
            for (Map.Entry<String, Object> entry : allUserInfoMap.entrySet()) {
                userInfoRow.setField(idx_userinfo, entry.getKey(), entry.getValue());
                idx_userinfo++;
            }
            System.out.println("userInfoRow:"+userInfoRow);
            userInfoRow.setTableName(USER_TABLE);
            kuduRowList.add(userInfoRow);

            // ADD 到子业务线用户表
            subUserInfoMap.put("linghit_id",queryUserMap.get("linghit_id").toString());
            subUserInfoMap.put("first_time", 1577808001);
            System.out.println("subUserInfoMap:"+subUserInfoMap);
            KuduRow userSubInfoRow = new KuduRow(subUserInfoMap.size());
            int idx_subinfo = 0;
            for (Map.Entry<String, Object> entry : subUserInfoMap.entrySet()) {
                userSubInfoRow.setField(idx_subinfo, entry.getKey(), entry.getValue());
                idx_subinfo++;
            }
            userSubInfoRow.setTableName(USER_SUB_TABLE);
            System.out.println("userSubInfoRow:"+userSubInfoRow);
            kuduRowList.add(userSubInfoRow);

        }else{  //索引表查到用户  更新
            System.out.println("索引表查到用户");
            linghit_id = queryUserMap.get("linghit_id");

            allUserInfoMap.put("linghit_id",linghit_id);
            System.out.println("allUserInfoMap:"+allUserInfoMap);
            KuduRow userInfoRow = new KuduRow(allUserInfoMap.size());
            int idx_userinfo = 0;
            for (Map.Entry<String, Object> entry : allUserInfoMap.entrySet()) {
                userInfoRow.setField(idx_userinfo, entry.getKey(), entry.getValue());
                idx_userinfo++;
            }
            System.out.println("userInfoRow:"+userInfoRow);
            userInfoRow.setTableName(USER_TABLE);
            kuduRowList.add(userInfoRow);
            System.out.println(kuduRowList);

            // Update 子业务线用户信息表相应字段
            subUserInfoMap.put("linghit_id",queryUserMap.get("linghit_id").toString());
            subUserInfoMap.put("first_time", 1577808001);
            System.out.println("subUserInfoMap:"+subUserInfoMap);
            KuduRow userSubInfoRow = new KuduRow(subUserInfoMap.size());
            int idx_subinfo = 0;
            for (Map.Entry<String, Object> entry : subUserInfoMap.entrySet()) {
                userSubInfoRow.setField(idx_subinfo, entry.getKey(), entry.getValue());
                idx_subinfo++;
            }
            userSubInfoRow.setTableName(USER_SUB_TABLE);
            System.out.println("userSubInfoRow:"+userSubInfoRow);
            kuduRowList.add(userSubInfoRow);
        }
        System.out.println("kuduRowList："+kuduRowList);
        return kuduRowList;
    }

    /**
     * user_info_link 处理逻辑
     * @param conn hbase连接对象
     * @param jsonObject 日志json
     */
    private static List<KuduRow> dealUserInfoLink(Connection conn, JSONObject jsonObject) {
        List<KuduRow> kuduRowList = new ArrayList<>();
        try {
            Map<String, Object> allUserInfoMap = LinghitUtil.getAllUserInfoMap(jsonObject);
            String user_id = jsonObject.getString("user_id");
            String record_date = DateUtil.parseCurrentTime();
            String log_time = jsonObject.getString(Constant.LINGHIT.log_time);
            String original_id = jsonObject.getString(Constant.LINGHIT.original_id);  //原id
            String target_id = jsonObject.getString(Constant.LINGHIT.target_id);      //目标id
            String app_id = jsonObject.getString(Constant.LINGHIT.app_id);
            String link_type = jsonObject.getString(Constant.LINGHIT.link_type);    //关联类型
            // link_type:cookie->openid，cookie关联微信openid
            // link_type:cookie->phone，cookie关联手机号
            // link_type:openid->phone，微信openid关联手机号
            // link_type:equipment_id->phone，设备号关联手机号
            String[] link_type_list = link_type.split("->");
            String origin_user_type = link_type_list[0];
            String origin_user_key = jsonObject.getString(origin_user_type);
            String target_user_type = link_type_list[1];
            String target_user_key = jsonObject.getString(target_user_type);

            // 根据初始用户标识查linghit_id
            Map<String,String> originIndexMap = HbaseDao.getByKey(conn, USER_INFO_INDEX_TABLE, origin_user_key);
            String origin_linghit_id = originIndexMap.get(Constant.LINGHIT.linghit_id);
            Map<String,String> targetIndexMap = HbaseDao.getByKey(conn, USER_INFO_INDEX_TABLE, target_user_key);
            String target_linghit_id = targetIndexMap.get(Constant.LINGHIT.linghit_id);

            //原用户标识索引查询为空，就返回不处理
            if(origin_linghit_id == null) return kuduRowList;

            // 目标用户标识是否命中索引
            if (null != target_linghit_id){
               /* //原用户索引是否有关联Phone
                if(originIndexMap.containsKey("$phone") && !StringUtils.isEmpty(originIndexMap.get("$phone"))) { //有关联

                } else {

                }*/

                // 索引表：更新origin的linghit_id为target的linghit_id
                TableName userIndexTable = TableName.valueOf(USER_INFO_INDEX_TABLE);
                HbaseDao.putRowColumnValue(conn, userIndexTable, origin_user_key, HBASEFAMILYCOLUMN, Constant.LINGHIT.linghit_id, target_linghit_id);
                // 用户表：更新target的linghit_id用户的origin用户标示字段（例如cookie）
                KuduRow userInfoRow = new KuduRow(allUserInfoMap.size());
                userInfoRow.setField(0,Constant.LINGHIT.linghit_id, target_linghit_id);
                // TODO first_time在日志里面没有
                userInfoRow.setField(1,Constant.LINGHIT.first_time, allUserInfoMap.get(Constant.LINGHIT.first_time));
                userInfoRow.setField(2,Constant.LINGHIT.app_id, app_id);
                userInfoRow.setField(3,origin_user_type, origin_user_type);
                userInfoRow.setTableName(USER_TABLE);
                kuduRowList.add(userInfoRow);
                System.out.println("kuduRowList:"+kuduRowList);

                // 用户表：删除origin的linghit_id用户
                userInfoRow.setDeleteOperator();
                userInfoRow.setField(0,Constant.LINGHIT.linghit_id, origin_linghit_id);
                // TODO first_time int
                userInfoRow.setField(1,Constant.LINGHIT.first_time, allUserInfoMap.get(Constant.LINGHIT.first_time));
                userInfoRow.setField(2,Constant.LINGHIT.app_id, app_id);
                userInfoRow.setTableName(USER_TABLE);
                kuduRowList.add(userInfoRow);

                // TODO 记录表：记录下需要更新的linghit_id

            }else {
                // 获取target用户标示
                String targetType = jsonObject.getString(Constant.LINGHIT.target_type);
                String userTargetType = jsonObject.getString(targetType);
                // 索引表：创建target的用户索引
                String linghit_id = jsonObject.getString(Constant.LINGHIT.linghit_id);
                TableName tableName = TableName.valueOf(USER_INFO_INDEX_TABLE);
                HbaseDao.putRowColumnValue(conn, tableName, linghit_id, HBASEFAMILYCOLUMN, targetType, userTargetType);
                // TODO 原用户索引是否关联target内容
                if(1==1){
                    // 索引表：更新origin的linghit_id为target的linghit_id
                    TableName userIndexTable = TableName.valueOf(USER_INFO_INDEX_TABLE);
                    HbaseDao.putRowColumnValue(conn, userIndexTable, origin_user_type, HBASEFAMILYCOLUMN, Constant.LINGHIT.linghit_id, target_linghit_id);

                    // 用户表：创建target用户
                    // ADD 到全用户信息表
                    allUserInfoMap.put("linghit_id", linghit_id);
                    allUserInfoMap.put("first_time", 1577808003);
                    KuduRow userInfoRow = new KuduRow(allUserInfoMap.size());
                    int idx_userinfo = 0;
                    for (Map.Entry<String, Object> entry : allUserInfoMap.entrySet()) {
                        userInfoRow.setField(idx_userinfo, entry.getKey(), entry.getValue());
                        idx_userinfo++;
                    }
                    System.out.println("userInfoRow:"+userInfoRow);
                    userInfoRow.setTableName(USER_TABLE);
                    kuduRowList.add(userInfoRow);

                }else{
                    // 索引表：更新origin的linghit_id用户的target字段，自增一列？
                    TableName userIndexTable = TableName.valueOf(USER_INFO_INDEX_TABLE);
                    HbaseDao.putRowColumnValue(conn, userIndexTable, origin_user_type, HBASEFAMILYCOLUMN, target_user_type, linghit_id);

                    // 用户表：更新origin的linghit_id用户的target字段
                    KuduRow userInfoRow = new KuduRow(allUserInfoMap.size());
                    userInfoRow.setField(0,Constant.LINGHIT.linghit_id, target_linghit_id);
                    // TODO first_time int
                    userInfoRow.setField(1,Constant.LINGHIT.first_time, allUserInfoMap.get(Constant.LINGHIT.first_time));
                    userInfoRow.setField(2,Constant.LINGHIT.app_id, app_id);
                    userInfoRow.setField(3,target_user_type, linghit_id);
                    userInfoRow.setTableName(USER_TABLE);
                    kuduRowList.add(userInfoRow);

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        KuduRow eventRow = new KuduRow(6);
        eventRow.setField(0, "log_id", jsonObject.get("log_id"));
        eventRow.setField(1, "log_time", jsonObject.get("log_time"));
        eventRow.setField(2, "app_id", "");
        eventRow.setField(3, "app", "");
        eventRow.setField(4, "event_id", "");
        eventRow.setField(5, "row_date", "");
        eventRow.setTableName(USER_EVENT_TABLE);
        kuduRowList.add(eventRow);
        return kuduRowList;
    }


    /**
     * user_event 处理逻辑
     * @param conn hbase连接对象
     * @param jsonObject 日志json
     */
    private static List<KuduRow> dealUserEvent(Connection conn, JSONObject jsonObject) {
        List<KuduRow> kuduRowList = new ArrayList<>();
        // 获取用户标示
        String targetType = jsonObject.getString(Constant.LINGHIT.target_type);
        String app_id = jsonObject.getString(Constant.LINGHIT.app_id);
        JSONObject attrObject = jsonObject.getJSONObject("attr");
        String targetValue = attrObject.getString(targetType);
        String userKey = targetType+":"+targetValue;
        // 根据用户标示查索引表
        Map<String, String> queryUserMap = HbaseDao.getByKey(conn, USER_INFO_INDEX_TABLE, userKey);
        System.out.println("queryUserMap:"+queryUserMap);

        Map<String, Object> allUserInfoMap = LinghitUtil.getAllUserInfoMap(jsonObject);
        Map<String, Object> subUserInfoMap = LinghitUtil.getSubUserInfoMap(jsonObject);
        Map<String, Object> userEventsMap = LinghitUtil.getUserEventsMap(jsonObject);
//        allUserInfoMap.put("app_id",app_id);
        System.out.println("==== "+allUserInfoMap);
        String linghit_id = queryUserMap.get(Constant.LINGHIT.linghit_id);

        if (queryUserMap.isEmpty() || !queryUserMap.containsKey("linghit_id") || StringUtils.isEmpty(queryUserMap.get("linghit_id"))){  //没有找到userKey的用户
            linghit_id = LinghitUtil.generateLinghitId();  //生成linghit_id
            // ADD 到索引表
            TableName userIndexName = TableName.valueOf(USER_INFO_INDEX_TABLE);
            HbaseDao.putRowColumnValue(conn, userIndexName, userKey, HBASEFAMILYCOLUMN, "linghit_id", linghit_id);
            // ADD 到反索引表
//            TableName userIndexReverseName = TableName.valueOf(USER_INFO_INDEX_REVERSE_TABLE);
//            HbaseDao.putRowColumnValue(conn, userIndexReverseName, linghit_id, HBASEFAMILYCOLUMN, targetType, targetValue);
            // ADD 到全司用户信息表
            allUserInfoMap.put("linghit_id", linghit_id);
            allUserInfoMap.put("first_time", 1577808008);
            KuduRow userInfoRow = new KuduRow(allUserInfoMap.size());
            int idx_userinfo = 0;
            for (Map.Entry<String, Object> entry : allUserInfoMap.entrySet()) {
                userInfoRow.setField(idx_userinfo, entry.getKey(), entry.getValue());
                idx_userinfo++;
            }
            System.out.println("userInfoRow:"+userInfoRow);
            userInfoRow.setTableName(USER_TABLE);
            kuduRowList.add(userInfoRow);

            // ADD 到子业务线用户表
            subUserInfoMap.put("linghit_id", linghit_id);
            subUserInfoMap.put("first_time", 1577808001);
            System.out.println("subUserInfoMap:"+subUserInfoMap);
            KuduRow userSubInfoRow = new KuduRow(subUserInfoMap.size());
            int idx_subinfo = 0;
            for (Map.Entry<String, Object> entry : subUserInfoMap.entrySet()) {
                userSubInfoRow.setField(idx_subinfo, entry.getKey(), entry.getValue());
                idx_subinfo++;
            }
            userSubInfoRow.setTableName(USER_SUB_TABLE);
            System.out.println("userSubInfoRow:"+userSubInfoRow);
            kuduRowList.add(userSubInfoRow);
        }

        // 添加到事件表
        userEventsMap.put("linghit_id", linghit_id);
        userEventsMap.put("log_time",1578499208);
        userEventsMap.put("app","linghit");
        KuduRow userEventRow = new KuduRow(userEventsMap.size());
        int idx_evevt = 0;
        for (Map.Entry<String, Object> entry : userEventsMap.entrySet()) {
            userEventRow.setField(idx_evevt, entry.getKey(), entry.getValue());
            idx_evevt++;
        }

        userEventRow.setTableName(USER_EVENT_TABLE);
        System.out.println("userEventRow:"+userEventRow);
        kuduRowList.add(userEventRow);
        System.out.println("kuduRowList:"+kuduRowList);

        return kuduRowList;
    }
}
