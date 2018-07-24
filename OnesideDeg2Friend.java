import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class De2Friends {
    public static class De2Mapper1 extends Mapper<Object,Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws
                IOException, InterruptedException {
            String line =value.toString();
            String[] strArr = line.split("\t");
            if(strArr.length==2) {
                //关注的人
                context.write(new Text(strArr[0]), new Text("1" + strArr[1]));
                //被关注的人
                context.write(new Text(strArr[1]), new Text("0" + strArr[0]));
            }
        }
    }

    public static class De2Reducer1 extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
           Set<String> follows= new HashSet<String>();
           Set<String> fans=new HashSet<String>();
           for(Text val :values ){
               String friend =val.toString();
               if(friend.startsWith("1")){
                   context.write(key,new Text(friend));//输出用户已经关注的人,一度人脉
                   follows.add(friend.substring(1));
               }
               if(friend.startsWith("0")){
                   fans.add(friend.substring(1));
               }
           }
           for(String fan : fans)
               for(String follow:follows) {
                   if (!fan.equals(follow)) {
                       context.write(new Text(fan),new Text("2"+follow));
                   }
               }

        }
    }

    public static class De2Mapper2 extends  Mapper<Object,Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line =value.toString();
            String[] strArr=line.split("\t");
            if(strArr.length==2) {
                context.write(new Text(strArr[0]), new Text(strArr[1]));//输出用户的一度好友和二度好友
            }
        }
    }

    public static class De2Reducer2 extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> firstFriend = new HashSet<String>();
            Set<String> secondFriend =new HashSet<String>();
            for(Text val:values){
                String friend =val.toString();
                if(friend.contains("1")){
                    firstFriend.add(friend.substring(1));
                }
                if(friend.contains("2")){
                    secondFriend.add(friend.substring(1));
                }
            }
            for(String second:secondFriend) {
                if(!(firstFriend.contains(second)))
                    context.write(key,new Text(second)); //输出好友的二度人脉
                }
        }
    }

    public static void main(String[] args) throws Exception{
        System.setProperty("hadoop.home.dir","E:\\softs\\majorSoft\\hadoop-2.7.5");
        Configuration conf =new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Path fileInput = new Path("hdfs://mycluster/testFile/qq.txt");
        Path tempDir = new Path("hdfs://mycluster/output/deg2friend-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Path fileOutput = new Path("hdfs://mycluster/output/qq");
        Job job = Job.getInstance(conf,"de2Firend");
        job.setJar("E:\\bigData\\hadoopDemo\\out\\artifacts\\wordCount_jar\\hadoopDemo.jar");
        job.setJarByClass(De2Friends.class);
        job.setMapperClass(De2Mapper1.class);
        job.setReducerClass(De2Reducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,fileInput);
        FileOutputFormat.setOutputPath(job,tempDir);
        job.waitForCompletion(true);//必须有，感觉是等job执行完才让job2执行的效果，即阻塞吧

        Job job2 = Job.getInstance(conf,"de2Firend");
        job2.setJar("E:\\bigData\\hadoopDemo\\out\\artifacts\\wordCount_jar\\hadoopDemo.jar");
        job2.setJarByClass(De2Friends.class);
        job2.setMapperClass(De2Mapper2.class);
        job2.setReducerClass(De2Reducer2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2,tempDir);
        FileOutputFormat.setOutputPath(job2,fileOutput);

        System.exit(job2.waitForCompletion(true)?0:1);
    }
}
