package shmu.speed2.speedup;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.bind.SchemaOutputResolver;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PmidCalMR {

//    static String cache = "F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\doc_2000-go.txt";
//    static String cache1 = "F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\2-goPair_Sim\\part-r-00000";
//    static String cache2 = "F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\doc_2000.txt";

    static String cache = "doc_20000-go.txt";
    static String cache1 = "part-r-00000";
    static String cache2 = "doc_20000.txt";

    /********************job3**********pmid go1;go2...转化成  go1 pmid1;pmid2...****************************/
    public static class Job3_PmIdGo_Shift_Map extends Mapper<LongWritable, Text,Text, Text> {
        Text k = new Text();
        Text v = new Text();
        ArrayList<String> All_go; //存放所有go
        @Override
        protected void setup(Context context) throws IOException {
            All_go = new ArrayList<>();
            BufferedReader br = new BufferedReader(new FileReader(cache)); //读入go文件
            String temp = "";
            while ((temp = br.readLine()) != null) {
                All_go.add(temp.trim());
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { //输入文档集
            String[] split = value.toString().split("\\s+");
            String pmId_c = split[0]; //读入pmid:c
            String[] gos = split[1].trim().split(";");
            ArrayList<String> GOS = new ArrayList<String>(Arrays.asList(gos));

            for (String go : All_go) {
                if (GOS.contains(go)) {
                    k.set(go);
                    v.set(pmId_c);
                    context.write(k,v);
                }
            }
        }
    }

    public static class Job3_PmIdGo_Shift_Reduce extends Reducer<Text,Text,Text,Text> {
        Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer strB = new StringBuffer();
            for (Text value : values) {
                strB.append(value.toString()+";");
            }
            String sub = strB.substring(0, strB.length() - 1);
            v.set(sub);
            context.write(key,v);
        }
    }

    /********************job4********************/
    public static class Job4_PmId_Sim_Map extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        Text k = new Text();
        DoubleWritable v = new DoubleWritable();
        HashMap<String, Float> goPair_sim;
        @Override
        protected void setup(Context context) throws IOException {  //保存go相似度文件
            goPair_sim = new HashMap<>();
            BufferedReader br = new BufferedReader(new FileReader(cache1));
            String temp;
            while ((temp = br.readLine()) != null) {
                String[] split = temp.split("\\s+");
                goPair_sim.put(split[0],Float.parseFloat(split[1]));
            }
        }
        //go pmid1;pmid2;...
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\s+");
            String goTerm = split[0];  //go术语
            String[] pmIdList = split[1].split(";");  //pmid:c列表

            BufferedReader br = new BufferedReader(new FileReader(cache2)); //文档集
            String temp;
            while ((temp = br.readLine()) != null) { //每次循环都获取到了这个goterm和文档之间的相似度
                String[] split1 = temp.split("\\s+");

                String pmId = split1[0].split(":")[0];  //pmid
                String count = split1[0].split(":")[1];
                String[] goList = split1[1].split(";");  //go列表

                float maxsim = 0;
                for (String goTerm1 : goList) { //循环取出当前文档的go
                    Float sim;
                    StringBuffer sbf = new StringBuffer("&");
                    if (Integer.valueOf(goTerm) < Integer.valueOf(goTerm1)) {
                        sbf.insert(0,goTerm);
                        sbf.append(goTerm1);
                    }else {
                        sbf.insert(0,goTerm1);
                        sbf.append(goTerm);
                    }
                    sim = goPair_sim.get(sbf);
                    if (sim != null) {
                        maxsim = Math.round(Math.max(sim, maxsim)*1000)/1000;
                    }

                    if(maxsim!=0){
                        v.set(maxsim); //最大相似度
                        for(String docname : pmIdList){
                            String pmId1 = docname.split(":")[0];
                            String count1 = docname.split(":")[1];
                            int len = Integer.valueOf(count) + Integer.valueOf(count1);
                            if (pmId.equals(pmId1)) {
                                len = len/2;
                            }
                            if (pmId1.compareTo(pmId)<0){
                                k.set(pmId1+"&"+pmId+":"+len);
                            }else{
                                k.set(pmId+"&"+pmId1+":"+len);
                            }
                            context.write(k,v); //<pmid1和pmid2，最大相似度>
                        }
                    }
                }
            }
            br.close();
        }
    }

    public static class Job4_PmId_Sim_Combiner extends Reducer<Text, DoubleWritable,Text, DoubleWritable>{
        DoubleWritable v = new DoubleWritable();
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            double x;
            for(DoubleWritable value : values){ //取出相似度进行相加
                sum += value.get();
            }
            x = (double)Math.round(sum*1000)/1000;
            v.set(x);
            context.write(key,v); //<标题和文档,>
        }
    }

    public static class Job4_PmId_Sim_Reduce extends Reducer<Text, DoubleWritable,Text, DoubleWritable>{
        DoubleWritable v = new DoubleWritable();
        Text k = new Text();
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            double x;
            for(DoubleWritable tmp : values){
                sum += tmp.get(); //相似度求和
            }
            String pmidpair = key.toString().split(":")[0];
            int c = Integer.valueOf(key.toString().split(":")[1]);
            x = (double)Math.round(sum/c*1000)/1000;//相似度/n
            k.set(pmidpair);
            v.set(x);
            context.write(k,v);
        }
    }
}
