package com.ydy.hbaseApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Created by ydy on 2016/6/12.
 */
public class HbaseApi {
    private  Configuration conf = null;


    public HbaseApi() {
        conf = HBaseConfiguration.create();
    }
    public static void main(String[] args) {
        HbaseApi hba = new HbaseApi();
        try {
            System.out.println("--------------------------------------------------------");
            hba.getOneRow_new("user","aaa");
            System.out.println("--------------------------------------------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public  boolean isExist(String tableName) throws IOException {
        HBaseAdmin ha = new HBaseAdmin(conf);
        return ha.tableExists(tableName);
    }

    public  void createTable(String tableName, String[] columnFamilies, int[] versions) throws IOException {
        if (isExist(tableName)) {
            HBaseAdmin hAdmin = new HBaseAdmin(conf);
            if (hAdmin.tableExists(tableName)) {
                System.out.println("表" + tableName + "已经存在");
                System.exit(0);
            } else {
                TableName tn = TableName.valueOf(tableName);
                HTableDescriptor tableDesc = new HTableDescriptor(tn);
                for(int i =0;i<columnFamilies.length;i++) {
                    HColumnDescriptor hcd = new HColumnDescriptor(columnFamilies[i]);
                    if (versions[i] == 0) {
                        hcd.setMaxVersions(1);
                    } else {
                        hcd.setMaxVersions(versions[i]);
                    }
                    tableDesc.addFamily(hcd);
                }
                hAdmin.createTable(tableDesc);
                hAdmin.close();
            }
        }
    }

    public  void dropTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("success");
        } else {
            System.out.println("the table is not exist");
            System.exit(0);
        }
        admin.close();
    }

    public void disableTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
        } else {
            System.out.println("the table is not exist");
            System.exit(0);
        }
    }

    public void enableTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            admin.enableTable(tableName);
        } else {
            System.out.println("the table is not exist");
            System.exit(0);
        }
    }

    public String[] listTable() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        TableName[] tn = admin.listTableNames();
        String[] tablenames={} ;
        for (int i = 0; i<tn.length;i++) {
            tablenames[i] = tn[i].toString();
        }
        return tablenames;
    }

    public void deleteCF(String tableName, String[] cfs) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(tableName));
        admin.disableTableAsync(TableName.valueOf(tableName));
        for(int i =0;i<cfs.length;i++) {
            tableDesc.removeFamily(Bytes.toBytes(cfs[i]));
        }
        admin.enableTable(TableName.valueOf(tableName));
        admin.close();
    }

    public void addCF(String tableName, String[] cfs, int[] versions) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(tableName));
        admin.disableTable(TableName.valueOf(tableName));
        for(int i =0;i<cfs.length;i++) {
            HColumnDescriptor hcd = new HColumnDescriptor(cfs[i]);
            hcd.setMaxVersions(versions[i]);
            hcd.setKeepDeletedCells(true);
            tableDesc.addFamily(hcd);
            admin.modifyTable(TableName.valueOf(tableName), tableDesc);
        }
        admin.enableTable(TableName.valueOf(tableName));
        admin.close();
    }

    public void alterCFVersion(String tableName, String cf, int version) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(TableName.valueOf(tableName));
        HTableDescriptor desc = admin.getTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hcd = desc.getFamily(Bytes.toBytes(cf));
        hcd.setMaxVersions(version);
        admin.modifyTable(TableName.valueOf(tableName), desc);
        admin.enableTable(tableName);
        admin.close();
    }
    public  void addRow(String tableName, String rowkey, String columnFamily, String column, String value) throws IOException {
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        put.setDurability(Durability.SYNC_WAL);
        table.put(put);
        table.close();
    }

    public void deleteRow(String tableName, String rowkey, String columnFamily, String column) throws IOException {
        HTable table = new HTable(conf, tableName);
        Delete delete = delete = new Delete(Bytes.toBytes(rowkey));
        if (columnFamily != null && column == null ) {
            delete.deleteFamily(Bytes.toBytes(columnFamily));
        } else if (columnFamily != null && column != null) {
            delete.deleteColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        }
        table.delete(delete);
        table.close();
    }

    public void getOneRow_old(String tableName, String rowkey) throws IOException, JSONException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        JSONObject json = new JSONObject();
        //old method
        for (KeyValue kv : result.raw()) {
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }


    public void getOneRow_new(String tableName, String rowkey) throws IOException, JSONException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        parseResult(map);
    }

    public void parseResult(NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map) {
        for (byte[] key : map.keySet()) {
            System.out.print("CF=" + Bytes.toString(key));
        }
        for (NavigableMap<byte[], NavigableMap<Long, byte[]>> cols : map.values()) {
            for (byte[] key : cols.keySet()) {
                System.out.print(" ,COL=" + Bytes.toString(key));
            }
            for (NavigableMap<Long, byte[]> value : cols.values()) {
                for (Long ts : value.keySet()) {
                    System.out.print(" ,ts=" + ts);
                }
                for (byte[] val : value.values()) {
                    System.out.println(", val=" + Bytes.toString(val));
                }
            }
        }
    }

    public void getAllRows_new(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String rk = Bytes.toString(r.getRow());
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = r.getMap();
            parseResult(map);
        }
        rs.close();
    }
    public void getAllRows_old(String tableName) throws IOException {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        //old method
        for (Result r : rs) {
            for (KeyValue kv : r.raw()) {
                System.out.print(new String(kv.getRow()) + " " );
                System.out.print(new String(kv.getFamily()) + ":" );
                System.out.print(new String(kv.getQualifier()) + " " );
                System.out.print(kv.getTimestamp() + " " );
                System.out.println(new String(kv.getValue()));
            }
        }
    }

}
