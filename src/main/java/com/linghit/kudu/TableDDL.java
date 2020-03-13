package com.linghit.kudu;

import com.linghit.dao.KuduDao;
import com.linghit.util.DateUtil;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableDDL {

    private static final String linghit_all_userinfo = "linghit_all_userinfo";
    private static final String linghit_sub_userinfo = "linghit_sub_userinfo";
    private static final String linghit_all_userevent_kudu = "linghit_all_userevent_kudu";
    private static final String zxcs_userinfo = "zxcs_userinfo";
    private static final String zxcs_userflow_kudu = "zxcs_userflow_kudu";

    public static void main(String[] args) throws Exception {
        KuduClient client = KuduDao.getKuduClient();
//        // 创建统一平台全用户表
//        createUserTable(client, linghit_all_userinfo, TableScheme.linghit_all_info_map);
//        // 创建统一平台业务用户表
//        createUserTable(client, linghit_sub_userinfo, TableScheme.linghit_sub_info_map);
//        // 创建统一平台全事件表
//        createFlowTable(client, linghit_all_userevent_kudu, TableScheme.linghit_flow_map);

        // 创建spark对应在线用户表
//        createZxcsUserTable(client, zxcs_userinfo, TableScheme.zxcs_info_map);
//        dropTable(client, zxcs_userinfo);
        // 创建spark对应在线事件表
//        createZxcsFlowTable(client, zxcs_userflow_kudu, TableScheme.zxcs_flow_map);

////         写入新的列
//        Map<String, Type> addColumnMap = new HashMap<String, Type>();
//        addColumnMap.put("testKuduCli", Type.STRING);
//        addColumns(client, "impala::default.kudu_impala_test_table", addColumnMap);

        // 添加和删除分区
        List<String> partionList = new ArrayList<>();
//        partionList.add("2020-02-01:2020-02-02");
//        partionList.add("2020-02-02:2020-02-03");
//        partionList.add("2020-02-03:2020-02-04");
        partionList.add("2020-02-14:2020-02-15");
        addRangePartitions(client, zxcs_userflow_kudu, "log_time", partionList);
//        dropRangePartitions(client, zxcs_userflow_kudu, "log_time", partionList);
    }

    private static void createFlowTable(KuduClient client, String tableName, Map<String, Type> schemeMap) throws KuduException {
        Schema schema = new Schema(getColumnScheme(schemeMap));

        // Set up the partition schema, which distributes rows to different tablets by hash.
        // Kudu also supports partitioning by key range. Hash and range partitioning can be combined.
        // For more information, see http://kudu.apache.org/docs/schema_design.html.
        CreateTableOptions options = new CreateTableOptions();

        List<String> hashKeys = new ArrayList<>();
        hashKeys.add("log_id");
        hashKeys.add("app_id");
        int numBuckets = 20;
        options.addHashPartitions(hashKeys, numBuckets);

        options.setRangePartitionColumns(ImmutableList.of("log_time"));

        /**
         * 设置range的分区范围
         PARTITION 1578412800 <= VALUES < 1578499200,   -- '2020-01-08'
         PARTITION 1578499200 <= VALUES < 1578585600   -- '2020-01-09',
         * */
        int count = 1580918400;
        for (int i = 1; i < 3; i++) {
            PartialRow lower = schema.newPartialRow();
            lower.addInt("log_time", count);
            System.out.println("lower:" + count);
            PartialRow upper = schema.newPartialRow();
            count += 86400;
            upper.addInt("log_time", count);
            System.out.println("upper:" + count);
            options.addRangePartition(lower, upper);
        }

        // Create the table.
        client.createTable(tableName, schema, options);
        System.out.println("Created table " + tableName);
    }

    private static void createUserTable(KuduClient client, String tableName, Map<String, Type> schemeMap) throws KuduException {
        Schema schema = new Schema(getColumnScheme(schemeMap));

        // Set up the partition schema, which distributes rows to different tablets by hash.
        // Kudu also supports partitioning by key range. Hash and range partitioning can be combined.
        // For more information, see http://kudu.apache.org/docs/schema_design.html.
        CreateTableOptions options = new CreateTableOptions();

        List<String> hashKeys = new ArrayList<>();
        hashKeys.add("linghit_id");
        hashKeys.add("app_id");
        int numBuckets = 10;
        options.addHashPartitions(hashKeys, numBuckets);

        options.setRangePartitionColumns(ImmutableList.of("first_time"));

        /**
         * 设置range的分区范围
         PARTITION 1575129600 <= VALUES < 1577808000,   -- '2019-12',
         PARTITION 1577808000 <= VALUES < 1580486400,   -- '2020-01',
         PARTITION 1580486400 <= VALUES < 1582992000,   -- '2020-02',
         PARTITION 1582992000 <= VALUES < 1585670400,   -- '2020-03'
         * */
        int count = 1575129600;
        for (int i = 1; i < 3; i++) {
            PartialRow lower = schema.newPartialRow();
            lower.addInt("first_time", count);
            System.out.println("lower:" + count);
            PartialRow upper = schema.newPartialRow();
            count += 2678400;
            upper.addInt("first_time", count);
            System.out.println("upper:" + count);
            options.addRangePartition(lower, upper);
        }

        // Create the table.
        client.createTable(tableName, schema, options);
        System.out.println("Created table " + tableName);
    }

    private static void createZxcsFlowTable(KuduClient client, String tableName, Map<String, Type> schemeMap) throws KuduException {
        Schema schema = new Schema(getColumnScheme(schemeMap));

        // Set up the partition schema, which distributes rows to different tablets by hash.
        // Kudu also supports partitioning by key range. Hash and range partitioning can be combined.
        // For more information, see http://kudu.apache.org/docs/schema_design.html.
        CreateTableOptions options = new CreateTableOptions();

        options.setRangePartitionColumns(ImmutableList.of("log_time"));

        /**
         * 设置range的分区范围
         PARTITION 1578412800 <= VALUES < 1578499200,   -- '2020-01-08'
         PARTITION 1578499200 <= VALUES < 1578585600   -- '2020-01-09',
         * */
        int count = 1580918400;
        for (int i = 1; i < 3; i++) {
            PartialRow lower = schema.newPartialRow();
            lower.addInt("log_time", count);
            System.out.println("lower:" + count);
            PartialRow upper = schema.newPartialRow();
            count += 86400;
            upper.addInt("log_time", count);
            System.out.println("upper:" + count);
            options.addRangePartition(lower, upper);
        }

        // Create the table.
        client.createTable(tableName, schema, options);
        System.out.println("Created table " + tableName);
    }

    private static void createZxcsUserTable(KuduClient client, String tableName, Map<String, Type> schemeMap) throws KuduException {
        Schema schema = new Schema(getColumnScheme(schemeMap));

        CreateTableOptions options = new CreateTableOptions();

        options.setRangePartitionColumns(ImmutableList.of("start_time"));

        /**
         * 设置range的分区范围
         PARTITION 1575129600 <= VALUES < 1577808000,   -- '2019-12',
         PARTITION 1577808000 <= VALUES < 1580486400,   -- '2020-01',
         PARTITION 1580486400 <= VALUES < 1582992000,   -- '2020-02',
         PARTITION 1582992000 <= VALUES < 1585670400,   -- '2020-03'
         * */
        int count = 1575129600;
        for (int i = 1; i < 3; i++) {
            PartialRow lower = schema.newPartialRow();
            lower.addInt("start_time", count);
            System.out.println("lower:" + count);
            PartialRow upper = schema.newPartialRow();
            count += 2678400;
            upper.addInt("start_time", count);
            System.out.println("upper:" + count);
            options.addRangePartition(lower, upper);
        }

        // Create the table.
        client.createTable(tableName, schema, options);
        System.out.println("Created table " + tableName);
    }

    /**
     * 删除表
     */
    public static void dropTable(KuduClient client, String tableName) throws KuduException {
        client.deleteTable(tableName);
    }

    /**
     * 添加列
     *
     * @param tableName 表名
     * @param schemeMap 需要添加的列
     */
    public static void addColumns(KuduClient client, String tableName, Map<String, Type> schemeMap) throws KuduException {
        AlterTableOptions ato = new AlterTableOptions();
        for (Map.Entry<String, Type> entry : schemeMap.entrySet()) {
            ato.addNullableColumn(entry.getKey(), entry.getValue());
        }

        client.alterTable(tableName, ato);
    }

    /**
     * 添加分区
     *
     * @param tableName       表名
     * @param partitionColumn 分区的列名
     * @param partitionList   分区列表，:号分割，2020-02-01:2020-02-02
     */
    public static void addRangePartitions(KuduClient client, String tableName, String partitionColumn, List<String> partitionList) throws KuduException {
        KuduTable table = client.openTable(tableName);
        Schema schema = table.getSchema();

        AlterTableOptions ato = new AlterTableOptions();
        for (String partition : partitionList) {
            PartialRow lower = schema.newPartialRow();
            lower.addInt(partitionColumn, DateUtil.date2TimeStampForKudu(partition.split(":")[0]));
            PartialRow uppper = schema.newPartialRow();
            uppper.addInt(partitionColumn, DateUtil.date2TimeStampForKudu(partition.split(":")[1]));
            ato.addRangePartition(lower, uppper);
        }

        client.alterTable(tableName, ato);
    }


    /**
     * 删除分区
     *
     * @param tableName       表名
     * @param partitionColumn 分区的列名
     * @param partitionList   分区列表，:号分割，2020-02-01:2020-02-02
     */
    public static void dropRangePartitions(KuduClient client, String tableName, String partitionColumn, List<String> partitionList) throws KuduException {
        KuduTable table = client.openTable(tableName);
        Schema schema = table.getSchema();

        AlterTableOptions ato = new AlterTableOptions();
        for (String partition : partitionList) {
            PartialRow lower = schema.newPartialRow();
            lower.addInt(partitionColumn, DateUtil.date2TimeStampForKudu(partition.split(":")[0]));
            PartialRow uppper = schema.newPartialRow();
            uppper.addInt(partitionColumn, DateUtil.date2TimeStampForKudu(partition.split(":")[1]));
            ato.dropRangePartition(lower, uppper);
        }

        client.alterTable(tableName, ato);
    }


    private static List<ColumnSchema> getColumnScheme(Map<String, Type> schemeMap) {
        List<ColumnSchema> columns = new ArrayList<>();

        for (Map.Entry<String, Type> column : schemeMap.entrySet()) {
            String columnName = column.getKey();
            if (columnName.startsWith("key::")) {
                columns.add(new ColumnSchema.ColumnSchemaBuilder(columnName.replace("key::", ""), column.getValue()).key(true).build());
            } else {
                columns.add(new ColumnSchema.ColumnSchemaBuilder(columnName, column.getValue()).nullable(true).build());
            }
        }

        return columns;
    }

}
