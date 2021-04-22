import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class operatorTableStudent {
    public static Configuration conf;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.1.101");
        conf.set("hbase.zookeeper.property.clientPort","2181");
    }
    public static boolean isTableExist(String tablename) throws IOException {
        ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tablename);
    }
    public static void addRowData(String tableName, String rowKey,String columnFamily,String column,
                                  String value) throws IOException {
        HTable hTable = new HTable(conf,tableName);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("insert success");
    }

    public static void main(String[] args) throws IOException {
        //添加一行数据
        addRowData("Student","scofiled","score","English","45");
        addRowData("Student","scofiled","score","Math","89");
        addRowData("Student","scofiled","score","EComputer","100");

    }
}
