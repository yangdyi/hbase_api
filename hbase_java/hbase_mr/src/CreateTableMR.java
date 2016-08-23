
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * Created by ydy on 2016/8/1.
 */
public class CreateTableMR {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] items = value.toString().split(",");
            if (items.length == 16) {
                String rk = items[1] + "_" + items[9];
                context.write(new Text(rk), value);
            }else{
                context.write(new Text("error_data"),value);
            }
        }
    }

    public static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (key.toString() != "error_data") {
                String rowkey = key.toString();
                Put put = new Put(Bytes.toBytes(rowkey));
                for (Text item : values) {
                    String[] items = item.toString().split(",");
                    put.add(Bytes.toBytes("user"), Bytes.toBytes("uname"), Bytes.toBytes(items[3]));
                    put.add(Bytes.toBytes("user"), Bytes.toBytes("nickname"), Bytes.toBytes(items[2]));
                    put.add(Bytes.toBytes("user"), Bytes.toBytes("phonenum"), Bytes.toBytes(items[4]));
                    put.add(Bytes.toBytes("user"), Bytes.toBytes("company"), Bytes.toBytes(items[0]));
                    put.add(Bytes.toBytes("good"), Bytes.toBytes("gname"), Bytes.toBytes(items[5]));
                    put.add(Bytes.toBytes("good"), Bytes.toBytes("gid"), Bytes.toBytes(items[8]));
                    put.add(Bytes.toBytes("good"), Bytes.toBytes("unitprice"), Bytes.toBytes(items[6]));
                    put.add(Bytes.toBytes("good"), Bytes.toBytes("count"), Bytes.toBytes(items[7]));
                    put.add(Bytes.toBytes("good"), Bytes.toBytes("ship"), Bytes.toBytes(items[10]));
                    put.add(Bytes.toBytes("location"), Bytes.toBytes("area"), Bytes.toBytes(items[11]));
                    put.add(Bytes.toBytes("location"), Bytes.toBytes("province"), Bytes.toBytes(items[12]));
                    put.add(Bytes.toBytes("location"), Bytes.toBytes("city"), Bytes.toBytes(items[13]));
                    put.add(Bytes.toBytes("location"), Bytes.toBytes("county"), Bytes.toBytes(items[14]));
                    put.add(Bytes.toBytes("location"), Bytes.toBytes("address"), Bytes.toBytes(items[15]));
                }
                context.write(new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(TableName.valueOf("table_2014"))) {
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("table_2014"));
            HColumnDescriptor coldesc1 = new HColumnDescriptor("user");
            HColumnDescriptor coldesc2 = new HColumnDescriptor("good");
            HColumnDescriptor coldesc3 = new HColumnDescriptor("location");
            desc.addFamily(coldesc1);
            desc.addFamily(coldesc2);
            desc.addFamily(coldesc3);
            admin.createTable(desc);
        }
        admin.close();
        Job job = new Job(conf, "mrcreatetable");
        job.setJarByClass(CreateTableMR.class);

        Path input = new Path(args[0]);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, input);

        TableMapReduceUtil.initTableReducerJob("table_2014", MyReducer.class, job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
