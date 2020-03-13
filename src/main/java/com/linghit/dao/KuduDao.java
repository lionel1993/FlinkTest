package com.linghit.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.linghit.util.AppUtil;
import com.linghit.util.ConfigUtil;
import com.linghit.util.TextUtils;
import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.serde.DefaultListSerDe;
import org.apache.flink.connectors.kudu.connector.serde.DefaultSerDe;
import org.apache.flink.connectors.kudu.connector.serde.KuduSerialization;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KuduDao {

    private static String kudu_masters = "172.16.110.83:7051,172.16.110.88:7051,172.16.110.87:7051";
//    private static String kudu_masters = "emr-worker-1.cluster-154553:7051";

    private static KuduClient client;
    private static Map<String, KuduTable> tableMap = new HashMap<>();


    public static KuduClient getKuduClient() {
        if (client == null) {
            client = new KuduClient.KuduClientBuilder(kudu_masters).build();
        }
        return client;
    }


    private static KuduTable getTable(String tableName) {
        KuduTable table = tableMap.get(tableName);
        if (table == null) {
            try {
                table = client.openTable(tableName);
                tableMap.put(tableName, table);
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
        return table;
    }

    /*
    索引查询linghit_id，
        比如使用cookie索引查询linghit_all_userindex_cookie表，是否有对应的linghit_id、
     */
    public static String queryLinghitIdByIndex(String tableName, String indexName, String indexValue) {
        String linghit_id = null;
        KuduScanner scanner = null;

        try {
            KuduClient client = getKuduClient();

            KuduTable table = getTable(tableName);

            //要查询返回的列
            List<String> projectColumns = new ArrayList<String>();
            projectColumns.add("linghit_id");

            KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns);

            //构造条件
            KuduPredicate predicate1 = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn(indexName),
                    KuduPredicate.ComparisonOp.EQUAL, indexValue);

            builder.addPredicate(predicate1);

            scanner = builder.build();

            while (scanner.hasMoreRows()) {
                RowResultIterator results = null;
                try {
                    results = scanner.nextRows();
                } catch (KuduException e) {
                    e.printStackTrace();
                    return linghit_id;
                }
                /*
                  RowResultIterator.getNumRows()
                        获取此迭代器中的行数。如果您只想计算行数，那么调用这个函数并跳过其余的。
                        返回：此迭代器中的行数
                        如果查询不出数据则 RowResultIterator.getNumRows() 返回的是查询数据的行数，如果查询不出数据返回0
                 */
                // 每次从tablet中获取的数据的行数
                int numRows = results.getNumRows();
                //System.out.println("numRows count is : " + numRows);

                if (numRows > 0) {
                    while (results.hasNext()) {
                        RowResult result = results.next();
                        linghit_id = result.getString(0);
                        //System.out.println("linghit_id is : " + linghit_id);
                    }
                } else {
//                break;
                    continue;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
        return linghit_id;
    }

    /*
    linghitid查询cookie等值，
        比如使用linghit_id索引查询linghit_all_userindex_cookie表，是否有对应的cookie值，可能多个
     */
    public static List<String> queryValueByLinghitId(String tableName, String indexValue, String valueName) {
        List<String> valueList = new ArrayList<String>();
        KuduScanner scanner = null;
        try {

            KuduClient client = getKuduClient();

            KuduTable table = getTable(tableName);

            //要查询返回的列
            List<String> projectColumns = new ArrayList<String>();
            projectColumns.add(valueName);

            KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns);

            //构造条件
            KuduPredicate predicate1 = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn("linghit_id"),
                    KuduPredicate.ComparisonOp.EQUAL, indexValue);

            builder.addPredicate(predicate1);

            long startTime = System.currentTimeMillis();
            scanner = builder.build();
            System.out.println("bulid time :" + (System.currentTimeMillis() - startTime));

            startTime = System.currentTimeMillis();

            while (scanner.hasMoreRows()) {
                RowResultIterator results = null;

                results = scanner.nextRows();

                    /*
                      RowResultIterator.getNumRows()
                            获取此迭代器中的行数。如果您只想计算行数，那么调用这个函数并跳过其余的。
                            返回：此迭代器中的行数
                            如果查询不出数据则 RowResultIterator.getNumRows() 返回的是查询数据的行数，如果查询不出数据返回0
                     */
                // 每次从tablet中获取的数据的行数
                int numRows = results.getNumRows();
//                System.out.println("numRows count is : " + numRows);
                if (numRows > 0) {
                    while (results.hasNext()) {
                        RowResult result = results.next();
                        String value = result.getString(0);
                        System.out.println("value is : " + value);
                        valueList.add(value);
                    }
                } else {
                    continue;
                }
            }

            System.out.println("iterator time :" + (System.currentTimeMillis() - startTime));

        } catch (KuduException e) {
            e.printStackTrace();
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (KuduException e) {
                    e.printStackTrace();
                }
            }
        }

        return valueList;
    }


    /*
     传递表名，获取动态sink
     */
    public static KuduSink<List<KuduRow>> getSink(String[] tables) {

        Map<String, KuduTableInfo> tableInfoMap = new HashMap<String, KuduTableInfo>();
        Map<String, KuduSerialization<KuduRow>> serializerMap = new HashMap<String, KuduSerialization<KuduRow>>();

        for (String table : tables) {
            tableInfoMap.put(table, KuduTableInfo.Builder.open(table).build());
            //serializerMap.put(table,new DefaultSerDe() );
        }

        // create a writer configuration
        KuduWriterConfig writerConfig = KuduWriterConfig.Builder
                .setMasters(kudu_masters)
                //存在则更新，不存在则插入
                //.setUpsertWrite()
                //同步刷新，效率较低
                //.setStrongConsistency()
                //异步刷新模式
                .setEventualConsistency()
                .build();

        KuduSink<List<KuduRow>> sink = new KuduSink<List<KuduRow>>(writerConfig, tableInfoMap, new DefaultListSerDe());

        return sink;
    }


    /**
     * 获取kudu表的元数据信息, 从kudu的Table里头拿, 列名:类型
     */
    public static Map<String, Type> getTableColumnsForKudu(Schema schema) {
        Map<String, Type> map = new HashMap<>();
        List<ColumnSchema> list = schema.getColumns();
        for (ColumnSchema columnSchema : list) {
            map.put(columnSchema.getName(), columnSchema.getType());
        }

        return map;
    }

    /**
     * 获取kudu表的元数据信息, 从redis的元数据里头拿, 列名:类型
     */
    public static Map<String, Type> getTableColumnsForRedis(String app_id, boolean isUser) {
        return RedisDao.getTableColumnsForRedis(app_id, isUser);
    }

    /**
     * 表结构更新检查，不需要打开表
     * 需要重新打开表才能拿到最新的更改后表结构，所以有两种方式可以获取刚更新完的表结构
     * 1、重新打开表
     * 2、手动把新增字段补充返回
     *
     * @param table  Kudu表
     * @param app_id 应用id
     * @param isUser 是否是用户表
     */
    public static void checkColumn(KuduClient client, KuduTable table, String app_id, boolean isUser) throws KuduException {
        System.out.println("表1:" + table);
        String app = AppUtil.getApp(app_id);
        String tableType = isUser ? "userinfo" : "userevent";
        Map<String, Type> columnMap = getTableColumnsForKudu(table.getSchema());
//        System.out.println("添加前:" + columnMap);

        // 获取redis对应元数据信息
        Jedis jedis = RedisDao.getJedis();
        String res_str = jedis.get(app + "-" + tableType);
        System.out.println(app + "-" + tableType);
        System.out.println(res_str);

        if (TextUtils.isEmpty(res_str)) return;

        JSONObject object = JSON.parseObject(res_str);
        Map<String, Object> newMap = object.getInnerMap();

        // 检查并添加对应的列
        AlterTableOptions ato = new AlterTableOptions();
        for (Map.Entry<String, Object> entry : newMap.entrySet()) {
            if (!columnMap.containsKey(entry.getKey())) {
                addSingleColumn(ato, entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        client.alterTable(table.getName(), ato);
        // TODO 测试元数据更新
//        table = client.openTable(table.getName());
//        System.out.println("表2:" + table);
//        System.out.println("添加后:" + getTableColumns(table.getSchema()));
    }

    private static void addSingleColumn(AlterTableOptions ato, String columnName, String columnType) {
        Type type;
        columnType = columnType.trim();
        if ("int".equals(columnType)) {
            type = Type.INT32;
        } else if ("double".equals(columnType)) {
            type = Type.DOUBLE;
        } else if ("float".equals(columnType)) {
            type = Type.FLOAT;
        } else if ("string".equals(columnType)) {
            type = Type.STRING;
        } else {
            type = Type.STRING;
        }
        ato.addNullableColumn(columnName, type);
    }

}
