package com.linghit.stream;

import com.alibaba.fastjson.JSONObject;
import com.linghit.constant.Constant;
import com.linghit.dao.HbaseDao;
import com.linghit.dao.KuduDao;
import com.linghit.hbase.HbaseConnectionUtil;
import com.linghit.udf.*;
import com.linghit.util.ConfigUtil;
import com.linghit.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;



public class LinghitZxcsLogKudu {


    public static void main(String[] args) throws Exception {

        ConfigUtil.init();


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000); // 设置启动检查点 checkpoint的间隔毫秒数
//        env.setParallelism(6); // 设置并行度
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", ConfigUtil.getProperties("zookeeper_host"));
        props.setProperty("bootstrap.servers", ConfigUtil.getProperties("kafka_host"));

//        props.setProperty("group.id", ConfigUtil.getProperties("linghit_log_group"));
        //flink-kudu-group-test 测试消费者组
//                props.setProperty("group.id", ConfigUtil.getProperties("flink_log_group"));
        props.setProperty("group.id", ConfigUtil.getProperties("flink_zxcs_log_group"));
//        props.setProperty("group.id", ConfigUtil.getProperties("group"));
        props.setProperty("enable.auto.commit", "true");
        System.out.println("****配置设置成功****");


//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(ConfigUtil.getProperties("linghit_log_topic"),new SimpleStringSchema(),props);
        //dev-sdklog 测试主题
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(ConfigUtil.getProperties("flink_log_topic"),new SimpleStringSchema(),props);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(ConfigUtil.getProperties("flink_zxcs_log_topic"),new SimpleStringSchema(),props);
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(ConfigUtil.getProperties("topic"),new SimpleStringSchema(),props);
        DataStream<String> stream = env.addSource(consumer);

//        打印日志
//        stream.print().setParallelism(1);

        //构建表数组
        String[] tables = new String[]{
                "zxcs_userinfo",
                "zxcs_userflow_kudu"
        };

        //获取sink
        /*
            注意：sink的输入是列表，支持多个顺序的操作，每个KuduRow是一个操作。
            目前之前upset和delete操作,默认KuduRow的操作是upset
         */
        KuduSink<List<KuduRow>> sink = KuduDao.getSink(tables);
        try {
            stream.rebalance().map( new MapFunction<String, List<KuduRow>>() {

                @Override
                public List<KuduRow> map(String value) throws Exception {

                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String app_id = jsonObject.getString("app_id");

                    //只处理在线且app_id不为空的日志
//                    if (!StringUtils.isEmpty(app_id) && app_id.startsWith("A")) {
                    //新添业务线
                    if ((!StringUtils.isEmpty(app_id) && app_id.startsWith("A")) || (!StringUtils.isEmpty(app_id) && app_id.startsWith("1"))) {
                        System.out.println(jsonObject.toString());
                        List<KuduRow> kuduRows = writeIntoKudu(jsonObject);
//                    System.out.println(jsonObject.getString("log_type")+"******"+kuduRows.toString());
                        return kuduRows;
                    } else {
                        return null;
                    }

                }
//        }).print();
            }).addSink(sink);


            env.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static List<KuduRow> writeIntoKudu(JSONObject jsonObject) {
        try {
            List<KuduRow> kuduRowList = new ArrayList<KuduRow>();
            String log_type = jsonObject.getString(Constant.LINGHIT.log_type);
            String app_id = jsonObject.getString("app_id");
            JSONObject attrObject = jsonObject.getJSONObject("attr");
            String log_time = jsonObject.getString("log_time");
            String user_id = jsonObject.getString("user_id");
            String app = app_id.substring(0,1) + "00";



            if (log_type.equals("user_event")) {

                String original_id = jsonObject.getString("original_id");
                String event_id = jsonObject.getString("event_id");
                String event_name = EventUDF.evaluate(event_id);

                //kudu事件表rowkey app_id+user_id+log_id
                String log_id = jsonObject.getString("log_id");
                //若log_id为null，生成uuid
                if (StringUtils.isEmpty(log_id)){
                    log_id = UUID.randomUUID().toString();

                }


                HashMap<String, Object> allMap = new HashMap<>();
                allMap.put("row_date",DateUtil.parseTime(log_time));
                allMap.put("log_time",jsonObject.get("log_time"));


                if(!StringUtils.isEmpty(log_type))
                    allMap.put("log_type",log_type);
                if(!StringUtils.isEmpty(user_id))
                    allMap.put("user_id",user_id);
                if(!StringUtils.isEmpty(original_id))
                    allMap.put("original_id",original_id);
                if(!StringUtils.isEmpty(event_id))
                    allMap.put("event_id",event_id);


                allMap.put("type","new");
                allMap.put("app_id",app_id);
                allMap.put("event_name",event_name);
                allMap.put("log_id",log_id);

                allMap.put("app",app);

                //attr处理
                for (Map.Entry<String,Object> entry : attrObject.entrySet()) {
                    Object value = entry.getValue();
                    if(value != null && !StringUtils.isEmpty(value.toString())){
                        resolveAttrFileFlow(allMap,entry.getKey(),entry.getValue());
                    }
                }
//                allMap.toString();

                //循环处理allMap字段添加到KuduRow中,字段pos无特殊意义、计数，字段key、value对应即可
                //KuduRow row = new KuduRow(3);
//                row.setField(0,key,value);
//                row.setField(1,key,value);
//                row.setField(2,key,value);
                int pos = allMap.size()-1;
                KuduRow row = new KuduRow(allMap.size());
                for(Map.Entry<String,Object> entry : allMap.entrySet()){
                    Object value = entry.getValue();
                    if (value != null && StringUtils.isNotEmpty(value.toString())&&pos>=0) {
                        if("log_time".equals(entry.getKey())){
                            row.setField(pos--, entry.getKey(), Integer.parseInt(entry.getValue().toString()));
                        }else if("$order_price".equals(entry.getKey())){
                            row.setField(pos--, entry.getKey(), Double.parseDouble(entry.getValue().toString()));
                        }
                        else if("$pay_price".equals(entry.getKey())){
                            row.setField(pos--, entry.getKey(), Double.parseDouble(entry.getValue().toString()));
                        }
//                        else if("$pay_result".equals(entry.getKey())){
////                            row.setField(pos--, entry.getKey(), Integer.parseInt(entry.getValue().toString()));
////                            double v = Double.parseDouble(entry.getValue().toString());
////                            row.setField(pos--, entry.getKey(),(new Double(v)).intValue());
//
//
////                            double v = Double.parseDouble(entry.getValue().toString());
////                            System.out.println(v+"***");
////                            int i = (new Double(v)).intValue();
////                            System.out.println(i+"***");
////                            row.setField(pos--, entry.getKey(),i);
//                            row.setField(pos--, entry.getKey(), Integer.parseInt(entry.getValue().toString()));
//                        }
                        else if("$pay_result".equals(entry.getKey())){

                            int payresult = Integer.parseInt(entry.getValue().toString());

                            row.setField(pos--, entry.getKey(), (byte)payresult);
                        }
                        else {
                            row.setField(pos--, entry.getKey(), entry.getValue());
                        }

                    }

                }




                row.setTableName("zxcs_userflow_kudu");

                /*
                 如果表结构更新，则设置标志位为true,会重新获取
                 默认标志位为true
                 */
                //row.setTabelChangeFlag(true);

                kuduRowList.add(row);

                return kuduRowList;

            }
            else if(log_type.equals("user_info")) {

                HashMap<String, Object> allMap = new HashMap<>();
                allMap.put("type", "new");
                allMap.put("app_id", app_id);
                allMap.put("regist_time", 0);

                if(!StringUtils.isEmpty(user_id))
                    allMap.put("user_id", user_id);





                allMap.put("row_date", DateUtil.parseTime(log_time));
                allMap.put("start_time", jsonObject.get("log_time"));

                allMap.put("app",app);

                //attr处理
                for (Map.Entry<String, Object> entry : attrObject.entrySet()) {
                    Object value = entry.getValue();
                    if (value != null && !StringUtils.isEmpty(value.toString())) {
                        resolveAttrFile(allMap, entry.getKey(), entry.getValue());
                    }
                }

                //以app_id + user_id为rowkey，查询hbase用户索引表zxcs_userInfo_kuduIndex
                //不为空，说明用户已存在，用户信息allMap剔除不更新字段app_id,user_id,start_time,row_date
                String rowKey = app_id + user_id;
                System.out.println("rowkey-----------------------"+rowKey);
                Connection conn = HbaseConnectionUtil.getHbaseConnection();
                TableName userIndexTableName = TableName.valueOf("zxcs_userInfo_kuduIndex"); // 用户索引表

                //查询hbase
                Map<String,String> queryMap = null;
                try {
                    Table table = conn.getTable(userIndexTableName);
                    List<String> columsList = new ArrayList<>();
                    columsList.add("start_time");
                    columsList.add("app_id");
                    columsList.add("user_id");
                    queryMap=query(table,rowKey,columsList);

                }catch (Exception e){
                    e.printStackTrace();
                }

                //用户已存在，只更新kudu表必要字段，从map中移除用户表不更新字段（row_date,start_time,$channel）
                //不存在，解析map不更改，插入hbase索引，字段（app_id,user_id,start_time）
                if (queryMap != null && !queryMap.isEmpty()){
                    System.out.println("is exists--------------------------- "+queryMap.toString());
                    //rowkey=appid+userid，固不需要专门移除app_id，userid字段
                    allMap.remove("row_date");
                    allMap.remove("$channel");
                    allMap.put("start_time",Integer.parseInt(queryMap.get("start_time")));
                    allMap.put("app_id",queryMap.get("app_id"));
                    allMap.put("user_id",queryMap.get("user_id"));

                }else{
                    System.out.println("not exists--------------------------- "+queryMap.toString());
                    if (allMap.containsKey("app_id")){
                        String appId = allMap.get("app_id").toString();

                        HbaseDao.putRowColumnValue(conn,userIndexTableName,rowKey,Constant.LINGHIT.attr,"app_id",appId);
                    }else{
                        HbaseDao.putRowColumnValue(conn,userIndexTableName,rowKey,Constant.LINGHIT.attr,"app_id","");

                    }

                    if (allMap.containsKey("user_id")){
                        String userId = allMap.get("user_id").toString();
                        HbaseDao.putRowColumnValue(conn,userIndexTableName,rowKey,Constant.LINGHIT.attr,"user_id",userId);

                    }else{
                        HbaseDao.putRowColumnValue(conn,userIndexTableName,rowKey,Constant.LINGHIT.attr,"user_id","");

                    }

                    if (allMap.containsKey("start_time")){
                        String startTime = allMap.get("start_time").toString();
                        HbaseDao.putRowColumnValue(conn,userIndexTableName,rowKey,Constant.LINGHIT.attr,"start_time",startTime);

                    }else{
                        HbaseDao.putRowColumnValue(conn,userIndexTableName,rowKey,Constant.LINGHIT.attr,"start_time","");

                    }
                    System.out.println("not exists--------------------------- "+rowKey);

                }

                int pos = allMap.size()-1;
                KuduRow row = new KuduRow(allMap.size());
                for(Map.Entry<String,Object> entry : allMap.entrySet()){
                    Object value = entry.getValue();
                    if (value != null && StringUtils.isNotEmpty(value.toString())&&pos>=0) {
                        if("start_time".equals(entry.getKey())){
                            row.setField(pos--, entry.getKey(), Integer.parseInt(entry.getValue().toString()));
                        }else if("regist_time".equals(entry.getKey())){
                            row.setField(pos--, entry.getKey(), Integer.parseInt(entry.getValue().toString()));
                        }
                        else if("age".equals(entry.getKey())){
                            int a = Integer.parseInt(entry.getValue().toString());

                            row.setField(pos--, entry.getKey(), (byte)a);
                        } else {
                            row.setField(pos--, entry.getKey(), entry.getValue());
                        }
                    }

                }
//                System.out.println("user_info per etl :" + "\n"+ allMap.toString());


                row.setTableName("zxcs_userinfo");
                kuduRowList.add(row);

                System.out.println("kudurow"+ kuduRowList.toString());
                return kuduRowList;

            }
            else {
                return null;
            }

        } catch (Exception e) {
            System.out.println(" run here ,return null");
            e.printStackTrace();
            return null;
        }
    }

    /*
        user_event事件attr解析转化
     */
    private static void resolveAttrFileFlow(HashMap<String, Object> attrMap, String key, Object value) {
        if (Constant.LINGHIT.$app_version.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$channel.equals(key)) {
            attrMap.put(key, value.toString());
            String channel_name = ChannelUDF.evaluate(value.toString(), 1,"A");
            attrMap.put(Constant.LINGHIT.$channel_name, channel_name);
            attrMap.put(Constant.LINGHIT.$channel_group, ChannelUDF.evaluate(value.toString(), 2,"A"));
            attrMap.put(Constant.LINGHIT.$channel_group_name, ChannelUDF.evaluate(value.toString(), 3,"A"));
        }
        else if(Constant.LINGHIT.$referrer.equals(key)){
            attrMap.put(key,value.toString());
        }
        else if(Constant.LINGHIT.$network.equals(key)){
            attrMap.put(key,value.toString());
        }
        else if (Constant.LINGHIT.$ip.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$city.equals(key)) {
            String city = GeographyUDF.evaluate(value.toString());
            attrMap.put(key, city);
        } else if (Constant.LINGHIT.$province.equals(key)) {
            String province = GeographyUDF.evaluate(value.toString());
            attrMap.put(key, province);
        } else if (Constant.LINGHIT.$country.equals(key)) {
            String country = GeographyUDF.evaluate(value.toString());
            attrMap.put(key, country);
        }
        else if (Constant.LINGHIT.$phone_operator.equals(key)) {
            attrMap.put(key, PhoneOperatorUDF.evaluate(value.toString(), 1));
        } else if (Constant.LINGHIT.$phone_operator_name.equals(key)) {
            attrMap.put(key, PhoneOperatorUDF.evaluate(value.toString(), 2));
        }else if(Constant.LINGHIT.schannel.equals(key)){
            attrMap.put(key,value.toString());
        }
        //只有为web端，设备从userAgent出
        else if (Constant.LINGHIT.$useragent.equals(key)) {
            attrMap.put(key,value.toString());
            attrMap.put(Constant.LINGHIT.$equipment_brand,
                    EquipmentUDF.evaluate(UseragentUDF.evaluate(value.toString(), "equipment_brand"), 1)
            );
            attrMap.put(Constant.LINGHIT.$equipment_brand_name,
                    EquipmentUDF.evaluate(UseragentUDF.evaluate(value.toString(), "equipment_brand_name"), 2)
            );
            attrMap.put(Constant.LINGHIT.$equipment_code,
                    UseragentUDF.evaluate(value.toString(), "equipment_code")
            );

            attrMap.put(Constant.LINGHIT.$sys,
                    UseragentUDF.evaluate(value.toString(), "sys")
            );
            attrMap.put(Constant.LINGHIT.$sys_version,
                    UseragentUDF.evaluate(value.toString(), "sys_version")
            );
            attrMap.put(Constant.LINGHIT.$browser,
                    UseragentUDF.evaluate(value.toString(),"browser")
            );

        }else if(Constant.LINGHIT.$regist_way.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$login_way.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$url.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$title.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$scan_time.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$module.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$start_way.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$order_id.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$order_price.equals(key)){
            attrMap.put(key,PriceUDF.evaluate(value.toString()));
        }else if(Constant.LINGHIT.$goods_name.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$goods_price.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$goods_amount.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$pay_way.equals(key)){
            attrMap.put(key,value.toString());
        }else if(Constant.LINGHIT.$pay_price.equals(key)){
            attrMap.put(key,PriceUDF.evaluate(value.toString()));
        }
        else if(Constant.LINGHIT.$pay_result.equals(key)){
            attrMap.put(key,PayResultUDF.evaluate(value.toString()));
        }



    }

    /*
        user_info事件attr解析转化
     */
    private static void resolveAttrFile(HashMap<String, Object> attrMap, String key, Object value) {
        if (Constant.LINGHIT.$app_version.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$channel.equals(key)) {
            attrMap.put(key, value.toString());
            String channel_name = ChannelUDF.evaluate(value.toString(), 1,"A");
            attrMap.put(Constant.LINGHIT.$channel_name, channel_name);

            String channel_group = ChannelUDF.evaluate(value.toString(), 2,"A");
            String channel_group_name = ChannelUDF.evaluate(value.toString(), 3,"A");
            attrMap.put(Constant.LINGHIT.$channel_group, channel_group);
            attrMap.put(Constant.LINGHIT.$channel_group_name, channel_group_name);

        } else if (Constant.LINGHIT.$app_userid.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$openid.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$token_person.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$token_umeng.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$username.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$alias.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$birthday.equals(key)) {
            attrMap.put(key, value.toString());
            Integer age = AgeUDF.evaluate(value.toString());
            attrMap.put(Constant.LINGHIT.$age, age);
        } else if (Constant.LINGHIT.$gender.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$ip.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$city.equals(key)) {
            String city = GeographyUDF.evaluate(value.toString());
            attrMap.put(key, city);
        } else if (Constant.LINGHIT.$province.equals(key)) {
            String province = GeographyUDF.evaluate(value.toString());
            attrMap.put(key, province);
        } else if (Constant.LINGHIT.$country.equals(key)) {
            String country = GeographyUDF.evaluate(value.toString());
            attrMap.put(key, country);
        } else if (Constant.LINGHIT.$mail.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$phone.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$qq.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$wechat.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$work_status.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$profession.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$marital_status.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$screen_width.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$screen_height.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$screen_size.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$network.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$wifi_switch.equals(key)) {
            attrMap.put(key, value.toString());
        } else if (Constant.LINGHIT.$phone_operator.equals(key)) {
            attrMap.put(key, PhoneOperatorUDF.evaluate(value.toString(), 1));
        } else if (Constant.LINGHIT.$phone_operator_name.equals(key)) {
            attrMap.put(key, PhoneOperatorUDF.evaluate(value.toString(), 2));
        } else if(Constant.LINGHIT.$useragent.equals(key)){
            //只有为web端，设备从userAgent出
            attrMap.put(Constant.LINGHIT.$equipment_brand,
                    EquipmentUDF.evaluate(UseragentUDF.evaluate(value.toString(), "equipment_brand"), 1)
            );
            attrMap.put(Constant.LINGHIT.$equipment_brand_name,
                    EquipmentUDF.evaluate(UseragentUDF.evaluate(value.toString(), "equipment_brand_name"), 2)
            );
            attrMap.put(Constant.LINGHIT.$equipment_code,
                    UseragentUDF.evaluate(value.toString(), "equipment_code")
            );

            attrMap.put(Constant.LINGHIT.$sys,
                    UseragentUDF.evaluate(value.toString(), "sys")
            );
            attrMap.put(Constant.LINGHIT.$sys_version,
                    UseragentUDF.evaluate(value.toString(),"sys_version")
            );
            attrMap.put(Constant.LINGHIT.$browser,
                    UseragentUDF.evaluate(value.toString(), "browser")
            );

        } else if (Constant.LINGHIT.schannel.equals(key)) {
            attrMap.put(key, value.toString());
        }

    }

    /**
     * 查询对应表的数据
     *
     * @param table      表名
     * @param columnList 需要查询的字段列表
     */
    private static Map<String, String> query(Table table, String row_key, List<String> columnList) {
        Map<String, String> dataMap = new HashMap<>();
        try {
            Get g = new Get(Bytes.toBytes(row_key));
            g.setCacheBlocks(true);
            g.setMaxVersions(1);
            byte[] family_attr = Bytes.toBytes("attr");
            for (String column : columnList) {
                g.addColumn(family_attr, Bytes.toBytes(column));
            }
            Result res = table.get(g);


            if (res.isEmpty()) return dataMap;
            Cell[] cells = res.rawCells();
            if (cells.length == 0) return dataMap;

            // 遍历取出所查询的数据
            dataMap.put(Constant.LINGHIT.row_key, Bytes.toString(CellUtil.cloneRow(cells[0])));
            for (Cell cell : cells) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                dataMap.put(key, Bytes.toString(CellUtil.cloneValue(cell)));
            }

        }catch (Exception e){
            e.printStackTrace();
            System.out.println("query 异常");
        }

        return dataMap;
    }

}