package com.linghit.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Created by ygz on 2019/5/21.
 */
public class HbaseConnectionUtil {

    private static String hbaseZookeeperQuorum = "172.16.110.83,172.16.110.87,172.16.110.88";
    private static String hbaseZookeeperClinentPort = "2181";
    private static String hbaseMaster = "172.16.110.87:16000";
    private static Connection hbaseConnection = null;

    public static Connection getHbaseConnection() {
        if (hbaseConnection == null) {
            try {
                org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
                config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
                config.set("hbase.master", hbaseMaster);
                config.set("hbase.zookeeper.property.clientPort", hbaseZookeeperClinentPort);
                config.setInt("hbase.rpc.timeout", 20000);
                config.setInt("hbase.client.operation.timeout", 30000);
                config.setInt("hbase.client.scanner.timeout.period", 200000);
                config.setInt("hbase.client.ipc.pool.size", 20);  //连接数
                // TODO 测试
//                config.setInt("hbase.client.ipc.pool.size", 1);  //连接数
                //在HBase中Connection类已经实现了对连接的管理功能，所以我们不需要自己在Connection之上再做额外的管理。
                // 另外，Connection是线程安全的，而Table和Admin则不是线程安全的，因此正确的做法是一个进程共用一个Connection对象，而在不同的线程中使用单独的Table和Admin对象
                hbaseConnection = ConnectionFactory.createConnection(config);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return hbaseConnection;
    }

}
