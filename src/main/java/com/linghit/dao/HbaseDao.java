package com.linghit.dao;

import com.linghit.constant.Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2020/2/12.
 */
public class HbaseDao {

    /**
     * 根据rowKey查询数据
     * @param connection
     * @param tableName
     * @param regexKey
     * @return
     */
    public static Map<String,String> scanRegexRowKey(org.apache.hadoop.hbase.client.Connection connection, String tableName, String regexKey) {
        List<Cell> cellList = new ArrayList<>();
        Map<String,String> rowMap = new HashMap<>();
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regexKey));
            scan.setFilter(filter);
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                cellList = r.listCells();
            }
            for(Cell cell : cellList) {
                rowMap.put(Bytes.toString( cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rowMap;
    }


    /**
     * 单列插入
     * @param connection
     * @param tableName
     * @param rowKey
     * @param familyColumn 列簇名
     * @param column      字段名
     * @param value       字段值
     */
    public static void putRowColumnValue(Connection connection, TableName tableName, String rowKey, String familyColumn,String column, String value) {
        try {
            Table table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 批量插入
     * @param connection
     * @param tableName
     * @param rowKey
     * @param familyColumn   列簇名
     * @param columnNames  需插入的所有字段名
     * @param values     需插入的所有字段值
     */
    public static void putRowValueBatch(Connection connection, TableName tableName, String rowKey, String familyColumn, List<String> columnNames, List<String> values) {
        try {
            Table table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            for (int j = 0; j < columnNames.size(); j++) {
                if(StringUtils.isNotEmpty(values.get(j))){
                    put.addColumn(Bytes.toBytes(familyColumn), Bytes.toBytes(columnNames.get(j)), Bytes.toBytes(values.get(j)));
                }
            }
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * get查询对应表的数据
     */
    public static Map<String, String> getByKey(Connection connection, String tableName, String rowKey) {
        Map<String, String> dataMap = new HashMap<>();
        Table table;
        try{
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result  ret = table.get(get);
            Cell[] cells = ret.rawCells();
            if (cells.length == 0) {
                return dataMap;
            }
            for (Cell cell : cells) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                dataMap.put(key,value);
            }
        }catch (Exception e){
            System.out.println("查询hbase错误，rowKey："+rowKey);
            //e.printStackTrace();
        }
        return dataMap;
    }
}
