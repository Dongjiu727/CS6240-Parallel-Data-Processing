package org.CS6240_hw4;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Table;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.client.Connection;
public class HbaseConnection {

    public static Configuration conf = null;
     //Initialization
    static {
        conf = HBaseConfiguration.create();
    }

    // Create a table

    // Close the connection and admin after execution.
    public static void creatTable(String tableName, String[] columnFamily)
            throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {


            TableName tname = TableName.valueOf(tableName);
            TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tname);
            ColumnFamilyDescriptorBuilder columnDescBuilder = ColumnFamilyDescriptorBuilder
                    .newBuilder(Bytes.toBytes(columnFamily[0]))
                    .setBlocksize(32 * 1024);
            tableDescBuilder.setColumnFamily(columnDescBuilder.build());


            //HTableDescriptor ht = new HTableDescriptor(TableName.valueOf(tableName));
            //for (int i = 0; i < columnFamily.length; i++) {
            //    ht.addFamily(new HColumnDescriptor(Bytes.toBytes(columnFamily[i])));
            //}
            //ht.addFamily(new HColumnDescriptor(Bytes.toBytes(HBASE_TABLE_NAME)));

            // Checks if the table already exists in HBase.
            // If it does, it will disable and delete the table.
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
            }
            admin.createTable(tableDescBuilder.build());
        } catch (IOException e) {
            // Handle the IOException appropriately
            e.printStackTrace();
            throw e; // Re-throwing the IOException for handling in the caller method
        }
        /*
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
         */
    }


    //Delete a table

    public static void deleteTable(String tableName) throws Exception {
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {

            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));

        } catch (IOException e) {
            // Handle the IOException appropriately
            e.printStackTrace();
            throw e; // Re-throwing the IOException for handling in the caller method
        }
    }


     // Put (or insert) a row into a table.
    public static void addRecord(String tableName, String rowKey,
                                 String columnFamily, String qualifier, String value) throws Exception {
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf(tableName));
            //HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            //System.out.println("insert record " + rowKey + " to table "
            //        + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    // Delete record from a table.
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        //HTable table = new HTable(conf, tableName);
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("Delete " + rowKey + " finished");
    }
  */


    /*
    // Get a row from a table.
    public static Result getRecord (String tableName, String rowKey) throws IOException{
        //HTable table = new HTable(conf, tableName);
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);

        for(Cell cell : rs.rawCells()){
            System.out.print(new String(CellUtil.cloneRow(cell)) + " ");
            System.out.print(new String(CellUtil.cloneFamily(cell)) + ":");
            System.out.print(new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.print(cell.getTimestamp() + " ");
            System.out.println(new String(CellUtil.cloneValue(cell)));
        }
        return rs;
    }
     */

    /*
    public static ResultScanner getAllRecord (String tableName) {
        ResultScanner ss = null;
        try{
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ss = table.getScanner(s);
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }

        } catch (IOException e){
            e.printStackTrace();
        }
        return ss;
    }
     */
}