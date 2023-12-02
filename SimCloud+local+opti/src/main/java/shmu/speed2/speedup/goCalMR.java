package shmu.speed2.speedup;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class goCalMR {

//    static String cache = "F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\isa_new.txt";
//    static String cache1 = "F:\\DocOf\\磕盐total\\mapreduce框架设计和优化\\bigset\\partof_new.txt";

    static String cache = "isa_new.txt";
    static String cache1 = "partof_new.txt";

    /**************************************job2*********************************************/
    public static class Job2_map extends Mapper<LongWritable, Text,Text, Text> {
        HashMap<String,String> isA_goPath;
        HashMap<String,String> partOf_goPath;
        @Override
        protected void setup(Context context) throws IOException {
            isA_goPath = new HashMap<>();  //保存isa_new.txt
            partOf_goPath = new HashMap<>();  //保存partof_new.txt
            BufferedReader br = new BufferedReader(new FileReader(cache));
            String temp;
            while((temp = br.readLine())!=null) {
                String[] go_path = temp.split(":");
                isA_goPath.put(go_path[0].trim(),go_path[1].trim());
            }
            br.close();

            BufferedReader br1 = new BufferedReader(new FileReader(cache1));
            String temp1;
            while((temp1 = br1.readLine())!=null) {
                String[] go_path = temp1.split(":");
                partOf_goPath.put(go_path[0].trim(),go_path[1].trim());
            }
            br1.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException, NullPointerException {
            Text v2 = new Text();

            double simisa = 0;
            double simpartof = 0;

            String[] goTerm = value.toString().split("&");
            String go1 = goTerm[0].trim();
            String go2 = goTerm[1].trim();  //得到两个go术语
            String temp1 = isA_goPath.get(go1);
            String temp2 = isA_goPath.get(go2); //获取go对应的路径

            if (temp1 != null && temp2 != null) {
                String[] Go1_Isa_path = temp1.split(" ");
                String[] Go2_Isa_path = temp2.split(" ");
//                double sim1 = goSimCal.SP(Go1_Isa_path, Go2_Isa_path);
//                double sim2 = goSimCal.Li(Go1_Isa_path, Go2_Isa_path);
                double sim3 = goSimCal.LC(Go1_Isa_path, Go2_Isa_path);
                double sim4 = goSimCal.WP(Go1_Isa_path, Go2_Isa_path);

                if (sim4 == 1 || sim3 == 1) {
                    simisa = sim4;
                }else {
                    simisa = 0.6*sim4 + 0.4*sim3;
                }
//                simisa = sim4;
            }

            String temp11 = partOf_goPath.get(go1);
            String temp22 = partOf_goPath.get(go2); //获取go对应的路径
            if (temp11 != null && temp22 != null) {
                String[] Go1_partOf_path = temp11.split(" ");
                String[] Go2_partOf_path = temp22.split(" ");
//                double sim1 = goSimCal.SP(Go1_partOf_path, Go2_partOf_path);
                double sim2 = goSimCal.Li(Go1_partOf_path, Go2_partOf_path);
//                double sim3 = goSimCal.LC(Go1_partOf_path, Go2_partOf_path);
                double sim4 = goSimCal.WP(Go1_partOf_path, Go2_partOf_path);
                simpartof = 0.8*sim4 + 0.2*sim2;
//                simpartof = sim4;
            }else{
                simpartof = 0;
            }

            double v;
            v = simisa > simpartof ? simisa : simpartof;
//            if (simisa == 1 || simpartof ==0) {
//                v = simisa;
//            }else {
//                v = 0.2 * simisa + 0.8* simpartof;
//            }
            if (v != 0) {
                v2.set(String.valueOf(v));
                context.write(value,v2);
            }
        }
    }

    public static class Job2_reduce extends Reducer<Text, Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text v = new Text();
            for (Text value : values) {
                v = value;
            }
            context.write(key,v);
        }
    }

}
