
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by ydy on 2016/8/1.
 */
public class HBaseToHBase {
    public static class HBaseMapper extends TableMapper<Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        public void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            String rk = Bytes.toString(key.get());
            /*String value = new String(result.getValue(Bytes.toBytes("location"), Bytes.toBytes("province")));*/
            String value = new String(result.getValue(Bytes.toBytes("good"), Bytes.toBytes("gname")));
            if (value.contains("美瞳")) {
              /* String county = new String(result.getValue(Bytes.toBytes("location"), Bytes.toBytes("county")));
                if (!county.equals("") && county.equals(" ")) {
                    context.write(new Text(county), ONE);
                }*/
                context.write(new Text(value), ONE);
            }
        }
    }

    public static class HBaseReduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("count"), Bytes.toBytes("counts"), Bytes.toBytes(sum));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists("countys")) {
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("countys"));
            HColumnDescriptor cols = new HColumnDescriptor("count");
            desc.addFamily(cols);
            admin.createTable(desc);
        }
        admin.close();

        Job job = new Job(conf, "hbasetohbase");
        job.setJarByClass(HBaseToHBase.class);

        job.setMapperClass(HBaseMapper.class);
        job.setReducerClass(HBaseReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob("table_2014",scan,HBaseMapper.class,Text.class,IntWritable.class,job);
        TableMapReduceUtil.initTableReducerJob("countys", HBaseReduce.class, job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
