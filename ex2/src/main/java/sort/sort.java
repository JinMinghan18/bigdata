package sort;

import merge.Merge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class sort {
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable>{
        private static IntWritable data = new IntWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            //转换为int
            data.set(Integer.parseInt(line));
            context.write(data,new IntWritable(1));
        }
    }
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        private static IntWritable lnum = new IntWritable(1);

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for(IntWritable val:values){
                context.write(lnum,key);
                lnum = new IntWritable(lnum.get() + 1);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop101:9000");

        Job job = Job.getInstance(conf);
        job.setJarByClass(sort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path("hdfs://hadoop101:9000/ex2/sort_input"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop101:9000/ex2/sort_output"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
