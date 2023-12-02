package shmu.speed2.speedup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class PmidAssJobMR {

    public static class Job3_map1 extends Mapper<LongWritable, Text,Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text t1 = new Text();
            Text t2 = new Text();

            String[] pmId_go = value.toString().split("&");

            t1.set(pmId_go[0].trim());
            t2.set(pmId_go[1].trim());

            context.write(t1,t2);
        }
    }

    public static class Job3_reduce1 extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sBuffer = new StringBuffer("");

            Text text = new Text();
            Text text1 = new Text();

            for (Text value : values) {
                sBuffer.append(value);
                sBuffer.append(";");
            }
            sBuffer.deleteCharAt(sBuffer.length()-1);
            text.set(sBuffer+"");
            text1.set(key.toString());

            context.write(text1,text);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        String[] filePath = {"F:\\DocOf\\elink\\数据集\\mr\\1.txt",
                "F:\\DocOf\\elink\\数据集\\mr\\2.txt",
                "F:\\DocOf\\elink\\数据集\\mr\\3.txt",
                "F:\\DocOf\\elink\\数据集\\mr\\4.txt",
                "F:\\DocOf\\elink\\数据集\\mr\\5.txt",
                "F:\\DocOf\\elink\\数据集\\mr\\6.txt",
                "F:\\DocOf\\elink\\数据集\\mr\\7.txt"};

        int a = 0;
        JobConf conf = new JobConf(PmidAssJobMR.class);
        for (String s : filePath) {
            a +=1;
            /**************************计算文档集****************************/
            Job job3 = Job.getInstance(conf, "job3 pmId Assembly");

            job3.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(job3,new Path(s)); //GO ID对文件

            job3.setMapperClass(Job3_map1.class);
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);

            job3.setReducerClass(Job3_reduce1.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);

            job3.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job3,new Path("F:\\DocOf\\elink\\数据集\\mr\\" + a));

            boolean bl3 = job3.waitForCompletion(true);
        }
    }
}
