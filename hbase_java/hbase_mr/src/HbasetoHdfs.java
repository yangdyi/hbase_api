

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by ydy on 2016/8/1.
 */
public class HbasetoHdfs {
    public static class HbaseMapper extends TableMapper<Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        public void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            String rk = Bytes.toString(key.get());
            String value = new String(result.getValue(Bytes.toBytes("good"), Bytes.toBytes("gname")));
            if (value.contains("美瞳")) {
                context.write(new Text(value), ONE);
            }
        }
    }

    public static class HdfsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum +=i.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf, "hbasetohdfs");
        job.setJarByClass(HbasetoHdfs.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob("table_2014", scan, HbaseMapper.class, Text.class, IntWritable.class, job);

        job.setMapperClass(HbaseMapper.class);
        job.setReducerClass(HdfsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("hdfs:///tmp/tmpexample/hbasetohdfs/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
