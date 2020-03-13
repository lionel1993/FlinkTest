/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.connectors.kudu.connector.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connectors.kudu.connector.KuduRow;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.failure.DefaultKuduFailureHandler;
import org.apache.flink.connectors.kudu.connector.failure.KuduFailureHandler;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Internal
public class KuduWriter implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<String,KuduTableInfo> tableInfoMap;
    private final KuduWriterConfig writerConfig;
    private final KuduFailureHandler failureHandler;

    private transient KuduClient client;
    private transient KuduSession session;
    private transient Map<String,KuduTable> tableMap;
    private transient KuduTable currentTable;

    public KuduWriter(Map<String,KuduTableInfo> tableInfoMap, KuduWriterConfig writerConfig) throws IOException {
        this (tableInfoMap, writerConfig, new DefaultKuduFailureHandler());
    }
    public KuduWriter(Map<String,KuduTableInfo> tableInfoMap, KuduWriterConfig writerConfig, KuduFailureHandler failureHandler) throws IOException {
        this.tableInfoMap = tableInfoMap;
        this.writerConfig = writerConfig;
        this.failureHandler = failureHandler;

        this.client = obtainClient();
        this.session = obtainSession();
        this.tableMap = obtainTable();
    }

    private KuduClient obtainClient() {
        return new KuduClient.KuduClientBuilder(writerConfig.getMasters()).build();
    }

    private KuduSession obtainSession() {
        KuduSession session = client.newSession();
        session.setFlushMode(writerConfig.getFlushMode());
        return session;
    }

    /*
      批量获取多个table
     */
    private Map<String,KuduTable> obtainTable() throws IOException {

        Map<String,KuduTable> tableMap = new HashMap<String,KuduTable>();

        for(String tableName :  tableInfoMap.keySet()) {
            KuduTableInfo tableInfo = tableInfoMap.get(tableName);
            if (client.tableExists(tableName)) {
                tableMap.put(tableName,client.openTable(tableName));
            }
            else if (tableInfo.createIfNotExist()) {
                tableMap.put(tableName,client.createTable(tableName, tableInfo.getSchema(), tableInfo.getCreateTableOptions()));
            }else
                throw new UnsupportedOperationException("table not exists and is marketed to not be created");
        }
        return tableMap;
    }

//    public Schema getSchema(String tableName) {
//        return tableMap.get(tableName).getSchema();
//    }


    public void write(KuduRow row) throws IOException {
        checkAsyncErrors();

        //获取row对应的table
        //如果更改了表结构，重新获取
        if(row.isTabelChangeFlag()) {
            tableMap.put(row.getTableName(),client.openTable(row.getTableName()));
        }
        currentTable = tableMap.get(row.getTableName());

        final Operation operation = mapToOperation(row);
        final OperationResponse response = session.apply(operation);

        checkErrors(response,row);
    }

    public void flushAndCheckErrors() throws IOException {
        checkAsyncErrors();
        flush();
        checkAsyncErrors();
    }

    @VisibleForTesting
    public DeleteTableResponse deleteTable() throws IOException {
        String tableName = currentTable.getName();
        return client.deleteTable(tableName);
    }

    @Override
    public void close() throws IOException {
        try {
            flushAndCheckErrors();
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (Exception e) {
                log.error("Error while closing session.", e);
            }
            try {
                if (client != null) {
                    client.close();
                }
            } catch (Exception e) {
                log.error("Error while closing client.", e);
            }
        }
    }

    private void flush() throws IOException {
        session.flush();
    }

    private void checkErrors(OperationResponse response,KuduRow row) throws IOException {
        if (response != null && response.hasRowError()) {
            failureHandler.onFailure(Arrays.asList(response.getRowError()));
        } else {
            checkAsyncErrors(row);
        }
    }

    private void checkAsyncErrors(KuduRow row) throws IOException {
        if (session.countPendingErrors() == 0) return;

        List<RowError> errors = Arrays.asList(session.getPendingErrors().getRowErrors());

        failureHandler.onFailure(errors,row);
    }

    private void checkAsyncErrors() throws IOException {
        if (session.countPendingErrors() == 0) return;

        List<RowError> errors = Arrays.asList(session.getPendingErrors().getRowErrors());
        failureHandler.onFailure(errors);
    }

    private Operation mapToOperation(KuduRow row) {
        final Operation operation = obtainOperation(row);
        final PartialRow partialRow = operation.getRow();

        currentTable.getSchema().getColumns().forEach(column -> {
            String columnName = column.getName();
            if (!row.hasField(columnName)) {
                return;
            }
            Object value = row.getField(columnName);

            if (value == null) {
                partialRow.setNull(columnName);
            } else {
                Type type = column.getType();
                switch (type) {
                    case STRING:
                        partialRow.addString(columnName, (String) value);
                        break;
                    case FLOAT:
                        partialRow.addFloat(columnName, (Float) value);
                        break;
                    case INT8:
                        partialRow.addByte(columnName, (Byte) value);
                        break;
                    case INT16:
                        partialRow.addShort(columnName, (Short) value);
                        break;
                    case INT32:
                        partialRow.addInt(columnName, (Integer) value);
                        break;
                    case INT64:
                        partialRow.addLong(columnName, (Long) value);
                        break;
                    case DOUBLE:
                        partialRow.addDouble(columnName, (Double) value);
                        break;
                    case BOOL:
                        partialRow.addBoolean(columnName, (Boolean) value);
                        break;
                    case UNIXTIME_MICROS:
                        //*1000 to correctly create date on kudu
                        partialRow.addLong(columnName, ((Long) value) * 1000);
                        break;
                    case BINARY:
                        partialRow.addBinary(columnName, (byte[]) value);
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal var type: " + type);
                }
            }
        });
        return operation;
    }

    /*
         根据table和operator获取对应的操作类
     */
    private Operation obtainOperation(KuduRow row) {

        //switch (writerConfig.getWriteMode()) {
        switch (row.getOperator()) {

            case INSERT:
                return currentTable.newInsert();

            case UPDATE: return currentTable.newUpdate();

            case UPSERT: return currentTable.newUpsert();

            //支持delete
            case DELTE: return  currentTable.newDelete();
        }
        //默认
        return currentTable.newUpsert();
    }
}
