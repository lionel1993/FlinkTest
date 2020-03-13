import com.linghit.dao.KuduDao;
import com.linghit.util.ConfigUtil;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/*
 * A simple example of using the synchronous Kudu Java client to
 * - Create a table.
 * - Insert rows.
 * - Alter a table.
 * - Scan rows.
 * - Delete a table.
 */
public class KuduExample {

    private static final Double DEFAULT_DOUBLE = 12.345;
    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "localhost:7051");

    static void createExampleTable(KuduClient client, String tableName) throws KuduException {
        // Set up a simple schema.
        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        // Set up the partition schema, which distributes rows to different tablets by hash.
        // Kudu also supports partitioning by key range. Hash and range partitioning can be combined.
        // For more information, see http://kudu.apache.org/docs/schema_design.html.
        CreateTableOptions cto = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("key");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable(tableName, schema, cto);
        System.out.println("Created table " + tableName);
    }

    static void insertRows(KuduClient client, String tableName, int numRows) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
        for (int i = 0; i < numRows; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("key", i);
            // Make even-keyed row have a null 'value'.
            if (i % 2 == 0) {
                row.setNull("value");
            } else {
                row.addString("value", "value " + i);
            }
            session.apply(insert);
        }

        // Call session.close() to end the session and ensure the rows are
        // flushed and errors are returned.
        // You can also call session.flush() to do the same without ending the session.
        // When flushing in AUTO_FLUSH_BACKGROUND mode (the default mode recommended
        // for most workloads, you must check the pending errors as shown below, since
        // write operations are flushed to Kudu in background threads.
        session.close();
        if (session.countPendingErrors() != 0) {
            System.out.println("errors inserting rows");
            org.apache.kudu.client.RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
            org.apache.kudu.client.RowError[] errs = roStatus.getRowErrors();
            int numErrs = Math.min(errs.length, 5);
            System.out.println("there were errors inserting rows to Kudu");
            System.out.println("the first few errors follow:");
            for (int i = 0; i < numErrs; i++) {
                System.out.println(errs[i]);
            }
            if (roStatus.isOverflowed()) {
                System.out.println("error buffer overflowed: some errors were discarded");
            }
            throw new RuntimeException("error inserting rows to Kudu");
        }
        System.out.println("Inserted " + numRows + " rows");
    }

    static void scanTableAndCheckResults(KuduClient client, String tableName, int numRows) throws KuduException {
        KuduTable table = client.openTable(tableName);
        Schema schema = table.getSchema();

        // Scan with a predicate on the 'key' column, returning the 'value' and "added" columns.
        List<String> projectColumns = new ArrayList<>(2);
        projectColumns.add("key");
        projectColumns.add("value");
        projectColumns.add("added");
        int lowerBound = 0;
        KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(
                schema.getColumn("key"),
                KuduPredicate.ComparisonOp.GREATER_EQUAL,
                lowerBound);
        int upperBound = numRows / 2;
        KuduPredicate upperPred = KuduPredicate.newComparisonPredicate(
                schema.getColumn("key"),
                KuduPredicate.ComparisonOp.LESS,
                upperBound);
        KuduScanner scanner = client.newScannerBuilder(table)
                .setProjectedColumnNames(projectColumns)
                .addPredicate(lowerPred)
                .addPredicate(upperPred)
                .build();

        // Check the correct number of values and null values are returned, and
        // that the default value was set for the new column on each row.
        // Note: scanning a hash-partitioned table will not return results in primary key order.
        int resultCount = 0;
        int nullCount = 0;
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if (result.isNull("value")) {
                    nullCount++;
                }
                double added = result.getDouble("added");
                if (added != DEFAULT_DOUBLE) {
                    throw new RuntimeException("expected added=" + DEFAULT_DOUBLE +
                            " but got added= " + added);
                }
                resultCount++;
            }
        }
        int expectedResultCount = upperBound - lowerBound;
        if (resultCount != expectedResultCount) {
            throw new RuntimeException("scan error: expected " + expectedResultCount +
                    " results but got " + resultCount + " results");
        }
        int expectedNullCount = expectedResultCount / 2 + (numRows % 2 == 0 ? 1 : 0);
        if (nullCount != expectedNullCount) {
            throw new RuntimeException("scan error: expected " + expectedNullCount +
                    " rows with value=null but found " + nullCount);
        }
        System.out.println("Scanned some rows and checked the results");
    }


    static void insertRows2(KuduClient client, String tableName, int numRows) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
        SessionConfiguration.FlushMode mode = SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC;
        session.setFlushMode(mode);
        for (int i = 0; i < numRows; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            String app_id = "A00" + UUID.randomUUID().toString().replace("-", "");
//            int start_time = (int) (System.currentTimeMillis() / 1000);
//            System.out.println(app_id + ":" + start_time);
            row.addString("app_id", app_id);
//            row.addInt("start_time", start_time);
            row.addString("type", "type" + i);
            row.addByte("test_info_table", (byte) 1);
            OperationResponse response = session.apply(insert);
            System.out.println(response.getRowError());
        }
    }

    private static void scanTableScheme(KuduClient client, String tableName) throws KuduException {
        KuduTable table = client.openTable(tableName);
        Schema schema = table.getSchema();
        System.out.println(KuduDao.getTableColumnsForKudu(schema));
    }

    private static void checkColumn(KuduClient client, String tableName) throws KuduException {
//        KuduDao.checkColumnNeedOpenTable(client, tableName, "A01", true);
        System.out.println(KuduDao.getTableColumnsForRedis("A01", true));
    }

    public static void main(String[] args) throws Exception {
        ConfigUtil.init();
        KuduClient client = KuduDao.getKuduClient();
        insertRows2(client, "test_info_table", 1);
    }

//    public static void main(String[] args) throws Exception {
//        ConfigUtil.init();
//        System.out.println("-----------------------------------------------");
//        System.out.println("Will try to connect to Kudu master(s) at " + KUDU_MASTERS);
//        System.out.println("Run with -DkuduMasters=master-0:port,master-1:port,... to override.");
//        System.out.println("-----------------------------------------------");
//        String tableName = "java_example-" + System.currentTimeMillis();
////        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
//        KuduClient client = KuduDao.getKuduClient();
//
//        try {
////            createExampleTable(client, tableName);
////            scanTableScheme(client, "java_example-1580975328237");
////            checkColumn(client, tableName);
//
//            insertRows2(client, "test_info_table", 1);
////            int numRows = 150;
////            insertRows(client, tableName, numRows);
////
////            // Alter the table, adding a column with a default value.
////            // Note: after altering the table, the table needs to be re-opened.
////            AlterTableOptions ato = new AlterTableOptions();
////            ato.addColumn("added", org.apache.kudu.Type.DOUBLE, DEFAULT_DOUBLE);
////            client.alterTable(tableName, ato);
////            System.out.println("Altered the table");
//
////            scanTableAndCheckResults(client, tableName, numRows);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            try {
////                client.deleteTable(tableName);
////                System.out.println("Deleted the table");
//            } catch (Exception e) {
//                e.printStackTrace();
//            } finally {
//                try {
//                    client.shutdown();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }

}
