package merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Merge {
    public static class Map extends Mapper<Object, Text, Text, Text>{
        private static Text text = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            text = value;
            context.write(text,new Text(""));
        }
    }
    public static class Reduce extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop101:9000");

        Job job = Job.getInstance(conf);
        job.setJarByClass(Merge.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path("hdfs://hadoop101:9000/ex2/input"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop101:9000/ex2/output"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
