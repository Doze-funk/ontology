package shmu.speed2.speedup;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class goPairJobMR {

    static String cache = "F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\doc_30000-go.txt";

    public static class Job1_map1 extends Mapper<LongWritable, Text,Text, NullWritable> {
        ArrayList<String> goTermDis;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            goTermDis = new ArrayList<>();
            BufferedReader br = new BufferedReader(new FileReader(cache));
            String temp;
            while((temp = br.readLine())!=null) {
                goTermDis.add(temp.trim());
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text k = new Text();
            //遍历所有 GOTerm
            for (String goTerm : goTermDis) {
                StringBuffer sBuffer = new StringBuffer("&");
                if(Integer.valueOf(goTerm) > Integer.valueOf(value.toString())) {
                    sBuffer.insert(0,value);
                    sBuffer.append(goTerm);
                }else {
                    sBuffer.insert(0,goTerm);
                    sBuffer.append(value);
                }
                k.set(sBuffer+"");
                context.write(k,NullWritable.get());
            }
        }
    }

    public static class Job1_combiner1 extends Reducer<Text, NullWritable,Text,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class Job1_reduce1 extends Reducer<Text, NullWritable,Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //输入
        String Job1_input = "file:///F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\doc_30000-go.txt"; //go文件
        //输出
        String Job1_output = "file:///F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\1";

        JobConf conf = new JobConf(goPairJobMR.class);
        /**************************job1****************************/
        Job job1 = Job.getInstance(conf, "job1 go pair");
//        job1.setNumReduceTasks(10);

        job1.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job1,new Path(Job1_input));

        job1.setMapperClass(Job1_map1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);

        job1.setCombinerClass(Job1_combiner1.class);

        job1.setReducerClass(Job1_reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        job1.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job1,new Path(Job1_output));

        boolean bl1 = job1.waitForCompletion(true);
    }
}
