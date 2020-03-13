import com.linghit.constant.Constant;
import com.linghit.hbase.HbaseConnectionUtil;
import com.linghit.stream.LinghitZxcsLogKudu;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseTest {


    public static void main(String[] args) {
        System.out.println(writeIntoKudu("A65b298e231-cb5b-4456-945a-0b9a3d097e8a"));
    }

    public static String writeIntoKudu(String value) {
        try {


//            String tableName = "linghit_all_userevent_kudu";
//
//            KuduTable table = client.openTable(tableName);
//            KuduSession session = client.newSession();
            Connection conn = HbaseConnectionUtil.getHbaseConnection();
            TableName userIndexTableName = TableName.valueOf("zxcs_userInfo_kuduIndex"); // 用户表
            Table userIndexTable = conn.getTable(userIndexTableName);
            List<String> columsList = new ArrayList<>();
            columsList.add("$phone");
            Map<String, String> map = query(userIndexTable, value, columsList);
            System.out.println(map);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return "啥";
    }

    /**
     * 查询对应表的数据
     *
     * @param table      表名
     * @param columnList 需要查询的字段列表
     */
    private static Map<String, String> query(Table table, String row_key, List<String> columnList) throws Exception {
        Map<String, String> dataMap = new HashMap<>();

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

        return dataMap;
    }

}
