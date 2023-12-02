package shmu.speed2.speedup;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class FinalJob {
    /*
        args[] = {用途,数据集大小}
        输入 test,speed,purity
        输出 2000,5000,10000,20000,30000
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //HDFS
        //输入
        String Job2_input = "hdfs://master:9000/Lhxxer/Input/gopair_20000.txt"; //输入go pair文件
        String Job3_input = "hdfs://master:9000/Lhxxer/Input/doc_20000.txt"; //输出文档数据集
        //输出
        String Job2_output = "hdfs://master:9000/Lhxxer/"+ args[0] +"/"+ args[1] +"/2-goPair_Sim";
        String Job3_output = "hdfs://master:9000/Lhxxer/"+ args[0] +"/"+ args[1] +"/3-pmidPair";
        String Job4_output = "hdfs://master:9000/Lhxxer/"+ args[0] +"/"+ args[1] +"/4-pmidSim";
        //缓存
        String cache_isaFile = "hdfs://master:9000/Lhxxer/isa_new.txt";
        String cache_partFile = "hdfs://master:9000/Lhxxer/partof_new.txt";
        String cache_goFile = "hdfs://master:9000/Lhxxer/Input/doc_20000-go.txt"; //go文件

        //local
        //输入
//        String Job2_input = "file:///F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\gopair_2000.txt"; //输入go pair文件
//        String Job3_input = "file:///F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\doc_2000.txt"; //输出文档数据集
//        //输出
//        String Job2_output = "file:///F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\2-goPair_Sim";
//        String Job3_output = "file:///F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\3-pmidPair";
//        String Job4_output = "file:///F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\4-pmidSim";
//        //缓存
//        String cache_isaFile = "file:///F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\isa_new.txt";
//        String cache_partFile ="file:///F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\partof_new.txt";

        JobConf conf = new JobConf(FinalJob.class);
        /***************************job2****************************/
        Job job2 = Job.getInstance(conf, "job2 calc sim"); //相似度计算
        job2.setJarByClass(FinalJob.class);
        job2.addCacheFile(new Path(cache_isaFile).toUri()); ///
        job2.addCacheFile(new Path(cache_partFile).toUri()); ///

//        job2.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize",(long)(33*1024*1024)); //134M
//        job2.getConfiguration().setLong("mapreduce.task.io.sort.mb",500); //kvbuffer大小
//        job2.getConfiguration().setLong("mapreduce.task.io.sort.factor",1000); //最后一轮直接设置最大 , 直接传递给reduce  //设置同时有多少个文件向文件写入数据
//        job2.getConfiguration().set("mapreduce.reduce.input.buffer.percent","0.5");
//        job2.getConfiguration().set("mapreduce.map.output.compress", "true");

//        job2.getConfiguration().setLong("mapreduce.job.jvmnumtasks",20); //开启JVM重用
//        job2.getConfiguration().setLong("min.num.spill.for.combine",2);
//        job2.getConfiguration().set("mapreduce.job.reduce.slowstart.completedmaps","0.75");
//        job2.getConfiguration().set("mapreduce.map.output.compress.codec", "LzoCodec");
//        job2.getConfiguration().setLong("mapreduce.reduce.shuffle.parallelcopies", 10);

        job2.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job2,new Path(Job2_input));

        job2.setMapperClass(goCalMR.Job2_map.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(goCalMR.Job2_reduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job2,new Path(Job2_output));
        boolean bl2 = job2.waitForCompletion(true);

        /*************************job3****************************/
        Job job3 = Job.getInstance(conf, "job3 pmId pair");
        job3.setJarByClass(FinalJob.class);
        job3.addCacheFile(new Path(cache_goFile).toUri()); ///

//        job3.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize",(long)(35*1024)); //142k
//        job3.getConfiguration().setLong("mapreduce.task.io.sort.mb",500); //kvbuffer大小
//        job3.getConfiguration().setLong("mapreduce.task.io.sort.factor",1000); //最后一轮直接设置最大 , 直接传递给reduce  //设置同时有多少个文件向文件写入数据
//        job3.getConfiguration().set("mapreduce.reduce.input.buffer.percent","0.5");
//        job3.getConfiguration().set("mapreduce.map.output.compress", "true");

        job3.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job3,new Path(Job3_input));

        job3.setMapperClass(PmidCalMR.Job3_PmIdGo_Shift_Map.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setReducerClass(PmidCalMR.Job3_PmIdGo_Shift_Reduce.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        job3.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job3, new Path(Job3_output));
        boolean bl4 = job3.waitForCompletion(true);

        /**************************job4****************************/
        Job job4 = Job.getInstance(conf, "job4 pmId Sim");
        job4.setJarByClass(FinalJob.class);
        job4.addCacheFile(new Path(Job2_output + "/part-r-00000").toUri()); ///
        job4.addCacheFile(new Path(Job3_input).toUri()); ///

//        job4.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize",(long)(250*1024*1024)); //文件1000M
//        job4.getConfiguration().set("mapreduce.map.output.compress", "true");
//        job4.getConfiguration().set("mapred.child.java.opts", "-Xmx4096m");
//        job4.getConfiguration().set("mapreduce.map.java.opts", "");

        job4.setNumReduceTasks(10);

        job4.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job4,new Path(Job3_output));  ////
        job4.setMapperClass(PmidCalMR.Job4_PmId_Sim_Map.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(DoubleWritable.class);
        job4.setCombinerClass(PmidCalMR.Job4_PmId_Sim_Combiner.class);
        job4.setReducerClass(PmidCalMR.Job4_PmId_Sim_Reduce.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);

        job4.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job4, new Path(Job4_output));
        boolean bl5 = job4.waitForCompletion(true);
    }

}
