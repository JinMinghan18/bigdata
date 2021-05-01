package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class HbaseOpData {
    public static Configuration conf;
    public static Connection connection;
    public static Admin admin;
    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.1.101");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void close() throws IOException {
        try {
            if(admin!=null){
                admin.close();
            }
            if(connection!=null){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }


    }
    public static boolean isTableExist(String tablename) throws IOException {
        ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tablename);
    }
    public static boolean isFamilyExist(String tableName,String columnFamliy) throws IOException {
        if(isTableExist(tableName)){
            Table table = connection.getTable(TableName.valueOf(tableName));
            HTableDescriptor tableDescriptor = table.getTableDescriptor();
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            int flag=0;
            for(HColumnDescriptor columnDescriptor:columnFamilies){
                if(columnDescriptor.getNameAsString().equals(columnFamliy)){
                    flag=1;
                }
            }
            if (flag==1)
                return true;
            return false;
        }
        return false;
    }
    public static void dropTable(String tableName) throws IOException {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        if(isTableExist(tableName)){
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }
    }
    public static void createTable(String tableName,String[] fields) throws IOException {
        if(isTableExist(tableName)){
            dropTable(tableName);
        }
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for(String cf:fields){
            descriptor.addFamily(new HColumnDescriptor(cf));
        }
        hBaseAdmin.createTable(descriptor);
        System.out.println("create table "+ tableName +" successful");
    }
    public static void addRecord(String tableName,String row,String[] fields,String[] values) throws IOException {
        HTable table = new HTable(conf,tableName);
        List<Put> putList = new ArrayList<Put>();
        int num = 0;
        for(String field:fields){
            String cf = field.split(":")[0];
            String col = field.split(":")[1];
            String value = values[num];
            num++;
            Put put = new Put(row.getBytes());
            put.add(cf.getBytes(),col.getBytes(),value.getBytes());
            putList.add(put);
        }
        table.put(putList);
        table.close();
        System.out.println("Insert data success");
    }
    public static void scanColumn(String tableName,String column) throws IOException {
        HTable hTable = new HTable(conf,tableName);
        Scan scan = new Scan();
        String[] cols = column.split(":");
        if(cols.length==1){
            scan.addFamily(column.getBytes());
        }
        else{
            scan.addFamily(cols[0].getBytes());
            scan.addColumn(cols[0].getBytes(),cols[1].getBytes());
        }
        ResultScanner resultScanner = hTable.getScanner(scan);
        for(Result result: resultScanner){
            Cell[] cells = result.rawCells();
            System.out.print("RowKey\tFamily\tColumn\tValue\n");
            for(Cell cell:cells){
                System.out.print(Bytes.toString(CellUtil.cloneRow(cell))+"\t");
                System.out.print(Bytes.toString(CellUtil.cloneFamily(cell))+"\t");
                System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");
                System.out.print(Bytes.toString(CellUtil.cloneValue(cell))+"\n");
            }
        }
    }
    public static void modifyData(String tableName,String row,String column,String data) throws IOException {
        HTable hTable = new HTable(conf,tableName);
        Put put = new Put(row.getBytes());
        String[] cols = column.split(":");
        put.add(cols[0].getBytes(),cols[1].getBytes(),data.getBytes());
        hTable.put(put);
        hTable.close();
        System.out.println("modify table:"+ tableName +" rowKey:"+row +" success");
    }
    public static void deleteRow(String tableName,String row) throws IOException {
        HTable hTable = new HTable(conf,tableName);
        Delete delete = new Delete(row.getBytes());
        hTable.delete(delete);
        hTable.close();
        System.out.println("delete row:" + row + " success");
    }
    public static void main(String[] args) throws IOException {
        //創建新表
//        String[] fields = {"info1","info2"};
//        createTable("jmh",fields);
        //插入數據
//        String[] fields = {"info1:subject","info1:score"};
//        String[] values = {"Math","88"};
//        addRecord("jmh","1",fields,values);
        //查找指定列族內容
//        scanColumn("jmh","info1:subject");
        //修改數據
//        modifyData("jmh","1","info1:score","99");
        //刪除行
        deleteRow("jmh","1");
    }
}
