package com.linghit.util;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.shaded.com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Created by Administrator on 2020/2/7.
 */
public class KuduTableUtil {

    private static String KUDU_MASTERS = "172.16.110.83:7051,172.16.110.88:7051,172.16.110.87:7051";

    /**
     * 创建kudu表
     * @param tableName
     * @param hashKeys
     * @param columns
     * @param partitionColumn
     * @throws Exception
     */
    public static void createKuduTable(String tableName,List<String> hashKeys, List<ColumnSchema> columns, String partitionColumn) throws Exception{

        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
        Schema schema = new Schema(columns);
        CreateTableOptions options  = new CreateTableOptions();
        int numBuckets = 10;
        options.addHashPartitions(hashKeys, numBuckets);
        options.setRangePartitionColumns(ImmutableList.of(partitionColumn));

        // Create the table.
        client.createTable(tableName, schema, options);

    }

}
