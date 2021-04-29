package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;


public class hbaseOpration {
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
    public static void addNewFamily(String tableName,String columnFamily) throws IOException {
        HBaseAdmin hadmin = new HBaseAdmin(conf);
        if(!isTableExist(tableName)){
            System.out.println("table "+tableName+"doesn't exist");
            return;
        }
        if(isFamilyExist(tableName,columnFamily)){
            System.out.println("columnFamily "+ columnFamily + " already exist");
            return;
        }
        if(isTableExist(tableName) && !isFamilyExist(tableName,columnFamily)){
            HTable table = new HTable(conf,tableName);
            HTableDescriptor hTableDescriptor = new HTableDescriptor(table.getTableDescriptor());
            hadmin.disableTable(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            hadmin.modifyTable(tableName,hTableDescriptor);
            hadmin.enableTable(tableName);
        }
        if(isFamilyExist(tableName,columnFamily)){
            System.out.println("add ColumnFamily " + columnFamily + " success");
        }
        close();
    }
    public static void delFamily(String tableName,String columnFamily) throws IOException {
        HBaseAdmin hadmin = new HBaseAdmin(conf);
        if(!isTableExist(tableName)){
            System.out.println("table "+tableName+"doesn't exist");
            return;
        }
        if(!isFamilyExist(tableName,columnFamily)){
            System.out.println("columnFamily "+ columnFamily + " doesn't exist");
            return;
        }
        if(isFamilyExist(tableName,columnFamily)){
            HTable table = new HTable(conf,tableName);
            HTableDescriptor hTableDescriptor = new HTableDescriptor(table.getTableDescriptor());
            hadmin.disableTable(tableName);
            hTableDescriptor.removeFamily(Bytes.toBytes(columnFamily));
            hadmin.modifyTable(tableName,hTableDescriptor);
            hadmin.enableTable(tableName);
        }
        if(!isFamilyExist(tableName,columnFamily)){
            System.out.println("remove ColumnFamily " + columnFamily + " success");
        }
        close();
    }
    public static void addNewColumn(String tableName,String rowKey,String columnFamily,
                              String column,String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        table.put(put);
        table.close();
        System.out.println("insert to "+ tableName+" success");
    }
    public static void delColumn(String tableName,String columnFamily,String column) throws IOException {
        HTable table = new HTable(conf,tableName);
        Scan scan = new Scan();
        List<Delete> deleteList = new ArrayList<Delete>();
        ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner){
            Cell[] cells = result.rawCells();
            for(Cell cell:cells){
                if(Bytes.toString(CellUtil.cloneQualifier(cell)).equals(column)
                && Bytes.toString(CellUtil.cloneFamily(cell)).equals(columnFamily)){
                    Delete delete = new Delete(Bytes.toString(CellUtil.cloneRow(cell)).getBytes());
                    delete.addColumn(Bytes.toString(CellUtil.cloneFamily(cell)).getBytes(),Bytes.toString(CellUtil.cloneQualifier(cell)).getBytes());
                    deleteList.add(delete);
                }
            }
        }
        table.delete(deleteList);

    }
    //清空table方法1
    public static void truncateTable1(String tableName) throws IOException {
        HTable table = new HTable(conf,tableName);
        Scan scan = new Scan();
        List<Delete> deleteList = new ArrayList<Delete>();
        ResultScanner scanner = table.getScanner(scan);
        for(Result result:scanner){
            Cell[] cells = result.rawCells();
            for(Cell cell:cells){
                Delete delete = new Delete(Bytes.toString(CellUtil.cloneRow(cell)).getBytes());
                deleteList.add(delete);
            }
        }
        table.delete(deleteList);
    }
    public static void countRows(String tableName) throws IOException {
        HTable table = new HTable(conf,tableName);
        Scan scan = new Scan();
        List<Delete> deleteList = new ArrayList<Delete>();
        ResultScanner scanner = table.getScanner(scan);
        long count=0;
        for(Result result:scanner){
            Cell[] cells = result.rawCells();
            count++;
        }
        System.out.println("table "+ tableName +" has "+ count+" rows");

    }
    public static void list() throws IOException {
        HTableDescriptor hTableDescriptor[] = admin.listTables();
        for (HTableDescriptor hTableDescriptor1:hTableDescriptor){
            System.out.println(hTableDescriptor1.getTableName());
        }
        close();
    }
    public static void scanAll(String tableName) throws IOException {
        HTable htable = new HTable(conf,tableName);
        Scan scan = new Scan();
        ResultScanner results = htable.getScanner(scan);
        for(Result result:results){
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
    public static void main(String[] args) throws IOException {
        hbaseOpration hbaseOpration = new hbaseOpration();
        //查询所有表
//        list();
        //扫描整表
//        scanAll("jmh");
        //添加列族
//        addNewFamily("jmh","info2");
        //添加列
//        addNewColumn("jmh","1","info2","name","stu1");
        //删除列族
//        delFamily("jmh","info2");
        //删除列
//        delColumn("jmh","info","subject");
        //删除表
//        truncateTable1("jmh");
        //统计表行数
//        countRows("jmh");
    }

}
