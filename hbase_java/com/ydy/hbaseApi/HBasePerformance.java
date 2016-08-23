import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by ydy on 16-4-18.
 */
public class HBasePerformance {
    /*static Configuration hbaseConfig = null;
    public static HTablePool pool = null;
    public static String tableName = "author";
    static{
        Configuration HBASE_CONFIG = new Configuration();
        hbaseConfig = HBaseConfiguration.create(HBASE_CONFIG);
        pool = new HTablePool(hbaseConfig, 1000);
    }*/
    /*
     * 多线程环境下线程插入函数
     *
     * */
    /*public static void InsertProcess()throws IOException
    {
        long start = System.currentTimeMillis();
        HTable table = null;
        HTableInterface inter = pool.getTable(tableName);
        inter.setAutoFlush(false);
        //table = (HTable) inter.
        table.setAutoFlush(false);
        table.setWriteBufferSize(24*1024*1024);
        //构造测试数据
        List<Put> list = new ArrayList<Put>();
        int count = 1000000;
        byte[] buffer = new byte[256];
        Random rand = new Random();
        for(int i=0;i<count;i++)
        {
            Put put = new Put(String.format("row %d",i).getBytes());
            rand.nextBytes(buffer);
            put.add("info".getBytes(), null, buffer);
            //wal=false
            put.setWriteToWAL(false);
            list.add(put);
            if(i%1000000 == 0)
            {
                table.put(list);
                list.clear();
                table.flushCommits();
            }
        }
        table.close();
        long stop = System.currentTimeMillis();
        //System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writeBuffer+",count="+count);
        System.out.println("线程:"+Thread.currentThread().getId()+"插入数据："+count+"共耗时："+ (stop - start)*1.0/1000+"s");
    }
    *//*
     * Mutil thread insert test
     * *//*
    public static void MultThreadInsert() throws InterruptedException
    {
        System.out.println("---------开始MultThreadInsert测试----------");
        long start = System.currentTimeMillis();
        int threadNumber = 10;
        Thread[] threads=new Thread[threadNumber];
        for(int i=0;i<threads.length;i++)
        {
            threads[i]= new ImportThread();
            threads[i].start();
        }
        for(int j=0;j< threads.length;j++)
        {
            (threads[j]).join();
        }
        long stop = System.currentTimeMillis();

        System.out.println("MultThreadInsert："+threadNumber*1000000+"共耗时："+ (stop - start)*1.0/1000+"s");
        System.out.println("---------结束MultThreadInsert测试----------");
    }
    public static void main(String[] args)  throws Exception{
        // TODO Auto-generated method stub
        //SingleThreadInsert();
        //MultThreadInsert();
    }
    public static class ImportThread extends Thread{
        public void HandleThread()
        {
            //this.TableName = "T_TEST_1";
        }
        //
        public void run(){
            try{
                InsertProcess();
            }
            catch(IOException e){
                e.printStackTrace();
            }finally{
                System.gc();
            }
        }
    }*/
    public static void main(String[] args) throws IOException {
        Configuration hbaseConfig = HBaseConfiguration.create();
        String tableName = "author";
        HTable htable;
        try {
            String cf = "info";
            long num = 1000000;
            htable = new HTable(hbaseConfig, Bytes.toBytes(tableName));
            long start = System.currentTimeMillis();
            long count = 0;
            String rk = args[0];
            for (long j = 0; j < 10000; j++) {
                String rowkey = rk + "_"+ j ;
                for(long i=0;i<100;i++) {
                    String column = "col_" + i;
                    String value = "data" + i;
                    Put put = new Put(Bytes.toBytes(rowkey));
                    put.setWriteToWAL(false);
                    put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
                    htable.setAutoFlush(false);
                    htable.put(put);
                    count ++;
                }
            }
            long end = System.currentTimeMillis();
            System.out.println("Add data success!");
            System.out.println("插入" + count + "条数据,共耗时" + (end - start) + "毫秒");
            htable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void single() {
       /*
       Configuration hbaseConfig = HBaseConfiguration.create();
          String tableName = "author";
        HTable htable;
        try {
            String cf = "info";
            long num = 1000000;
            htable = new HTable(hbaseConfig, Bytes.toBytes(tableName));
            long start = System.currentTimeMillis();
            long count = 0;
            String rk = args[0];
            for (long j = 0; j < 10000; j++) {
                String rowkey = "ccrk"+j ;
                for(long i=0;i<100;i++) {
                    String column = "col_" + i;
                    String value = "data" + i;
                    Put put = new Put(Bytes.toBytes(rowkey));
                    put.setWriteToWAL(false);
                    put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
                    htable.setAutoFlush(false);
                    htable.put(put);
                    count ++;
                }
            }
            long end = System.currentTimeMillis();
            System.out.println("Add data success!");
            System.out.println("插入" + count + "条数据,共耗时" + (end - start) + "毫秒");
            htable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        */
    }
}

