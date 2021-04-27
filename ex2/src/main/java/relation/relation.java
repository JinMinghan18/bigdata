package relation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sort.sort;

import java.io.IOException;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;

public class relation {
    public static int time=0;
    public static class Map extends Mapper<Object, Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] CandP = line.split("\t");
            List<String> list = new ArrayList<>(2);
            for(String cp:CandP){
                if(!cp.equals("")){
                    list.add(cp);
                }
            }
            if(!list.get(0).equals("child")){
                String childName = list.get(0);
                String parentName = list.get(1);
                String relationReverse = "1";
                context.write(new Text(parentName),new Text(relationReverse + "+"
                        + childName + "+" + parentName));
                relationReverse = "2";
                context.write(new Text(childName),new Text(relationReverse + "+"
                        + childName + "+" + parentName));
            }
        }
    }
    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(time == 0){
                context.write(new Text("grandchild"),new Text("grandparent"));
                time++;
            }
            List<String> grandchild = new ArrayList<>();
            List<String> grandparent = new ArrayList<>();
            for (Text text:values){
                String s = text.toString();
                String[] relation = s.split("\\+");
                String relationType = relation[0];
                String childName = relation[1];
                String parentName = relation[2];
                if(relationType.equals("1")){
                    grandchild.add(childName);
                }
                else grandparent.add(parentName);
            }
            int grandPNum = grandparent.size();
            int grandCNum = grandchild.size();
            if(grandPNum !=0 && grandCNum!=0){
                for(int m = 0;m< grandCNum;m++){
                    for (int n = 0;n<grandPNum;n++){
                        context.write(new Text(grandchild.get(m)),new Text(grandparent.get(n)));
                    }
                }
            }

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop101:9000");

        Job job = Job.getInstance(conf);
        job.setJarByClass(relation.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path("hdfs://hadoop101:9000/ex2/relation_input"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://hadoop101:9000/ex2/relation_output"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
