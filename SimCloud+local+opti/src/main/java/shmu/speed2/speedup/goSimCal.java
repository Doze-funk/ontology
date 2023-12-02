package shmu.speed2.speedup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class goSimCal {

    public static double lca(String[] path1,String[] path2){ //寻找最小公共祖先的深度
        //如果开头不一样，则无共同父点
        if(!path1[0].equals(path2[0]))
            return 0.0;
        int p1l = path1.length;
        int p2l = path2.length;
        double ml=p1l < p2l ? p1l : p2l;
        int i=0;
        while(i!=ml){
            if(!path1[i].trim().equals(path2[i].trim())){
                break;
            }
            i=i+1;
        }
        return i;
    }

    public static double getdepth(String m,String[] ta,String[] tb) {
        double re=0.0;
        int mark=0;
        for(String temp:ta) {
            re++;
            if(temp.equals(m)) {
                mark=1;
                break;
            }
        }
        if(mark==1)
            return re;
        re=0;
        for(String temp:tb) {
            re++;
            if(temp.equals(m)) {
                break;
            }
        }
        return re;
    }

    //输出两个结点输出最大深度
    public static double maxdepth(String[] a,String[] b) {
        double max=0.0;
        for(int i=0;i<a.length;i++) {
            String[] temp=a[i].split("\\.");
            max=max > temp.length ? max : temp.length;
        }
        for(int i=0;i<b.length;i++) {
            String[] temp=b[i].split("\\.");
            max=max > temp.length ? max : temp.length;
        }
        return max;
    }

    public static double SP(String[] a, String[] b) {
        // (max-path_length(a, b))/max
        if(Arrays.toString(a).equals(Arrays.toString(b)))
            return 1;
        double min=1000;
        double max=0;
        int f1=a.length;
        int f2=b.length;
        for(int i=0;i<f1;i++){
            String[] arra=a[i].split("\\.");
            for(int j=0;j<f2;j++){
                String[] arrb=b[j].split("\\.");
//                double pathlength = pathlength(arra, arrb); //求得两个节点之间的最短路径长度
                double pathlength = arra.length+arrb.length-2*lca(arra,arrb);
                max = max > pathlength ? max : pathlength;
                min = pathlength < min ? pathlength : min;
            }
        }
        double d=(max-min)/max;
        return d;
    }

    public static double Li(String[] a, String[] b) {
        double d=0;//WP求最大相关性
        int f1=a.length;
        int f2=b.length;
        for(int i=0;i<f1;i++){
            String[] arra=a[i].split("\\.");
            for(int j=0;j<f2;j++){
                String[] arrb=b[j].split("\\.");
                double H=lca(arra, arrb);
                double L = arra.length+arrb.length-2*lca(arra,arrb);
//                double L=pathlength(arra, arrb);
                double v = Math.exp(-0.2 * L) * (Math.exp(0.6 * H) - Math.exp(-0.6 * H)) / (Math.exp(0.6 * H) + Math.exp(-0.6 * H));
                d=v > d ? v : d;
            }
        }
        return d;
    }

    //用到最大深度和两个节点之间的最短路径
    public static double LC(String[] a,String[] b) {
        double maxd=maxdepth(a,b);
        double k=1000;
        int f1=a.length;
        int f2=b.length;

        for(int i=0;i<f1;i++){
            for(int j=0;j<f2;j++){
                String[] arra = a[i].split("\\.");
                String[] arrb = b[j].split("\\.");
                if(!arra[0].equals(arrb[0])) {
                    double v = 0.0;
                    return v;
                }
                double pathlength = arra.length+arrb.length-2*lca(arra,arrb);
                k=pathlength < k ? pathlength : k;
            }
        }
        double d=1-Math.log(k+1)/Math.log(maxd*2+1);
        return d;
    }

    public static double pathlength(String[] a,String[] b) {
        //length(a)+length(b)-2*lca(a,b)
        return (a.length+b.length-2*lca(a,b)); //a数组长度加上b数组长度减两倍lca tq
    }

    public static double WP(String[] a,String[] b) { //两个 路径集合
        double d=0;//WP求最大相关性
        int f1=a.length;
        int f2=b.length;
        for(int i=0;i<f1;i++){ //路径集合1
            String[] arra=a[i].split("\\."); // 节点集合1
            for(int j=0;j<f2;j++){ //路径集合2
                String[] arrb=b[j].split("\\."); // 节点集合2
                double v = (2 * lca(arra, arrb)) / (arra.length + arrb.length);
                d = v > d ? v : d;
            }
        }
        return d;
    }

    public static double WL(String[] pathSetGo1_PartOf, String[] pathSetGo2_PartOf) {
        HashMap<Double,String> temp = new HashMap<>();
        double max = 0;
        double min = 1000;
        double maxsum=0.0;
        double minsum=10000.0;

        for (int i= 0;i < pathSetGo1_PartOf.length;i++) {
            String[] go1NodeSet = pathSetGo1_PartOf[i].split("\\.");
            for (int j = 0; j < pathSetGo2_PartOf.length; j++) {
                String[] go2NodeSet = pathSetGo2_PartOf[j].split("\\.");
                double lcs = lca(go1NodeSet, go2NodeSet);
                double pathlen = go1NodeSet.length + go2NodeSet.length - 2 * lcs;
                temp.put(pathlen , pathSetGo1_PartOf[i] + ":" + pathSetGo2_PartOf[j] + ":" + lcs); //两节点路径长度 : 两节点partof路径集合 : 最小公共节点深度
                max = max > pathlen ? max : pathlen; //最大路径长度
                min = min < pathlen ? min : pathlen; //最小路径长度
            }
        }

        for (double k: temp.keySet()) {
            if(k == max) {
                String[] split = temp.get(k).split(":");
                ArrayList<String> tempx=new ArrayList<String>();
                String[] ta=split[0].split("\\.");
                String[] tb=split[1].split("\\.");
                double ans=Double.parseDouble(split[2]);
//                double ans=lca(ta,tb);
                for(int j=(int)(ans+1);j<ta.length;j++) {
                    tempx.add(ta[j].trim());
                }
                for(int j=(int)ans;j<tb.length;j++) {
                    tempx.add(tb[j].trim());
                }
                double sum=0.0;
                for(int j=0;j<tempx.size();j++) {
                    sum=sum+1/getdepth(tempx.get(j),ta,tb);
                }
                maxsum = sum > maxsum ? sum : maxsum;
            }
            if (k == min) {
                String[] split = temp.get(k).split(":");
                ArrayList<String> tempx=new ArrayList<String>();
                String[] ta=split[0].split("\\.");
                String[] tb=split[1].split("\\.");
                double ans=Double.parseDouble(split[2]);
//                double ans=lca(ta,tb);
                for(int j=(int)(ans+1);j<ta.length;j++) {
                    tempx.add(ta[j].trim());
                }
                for(int j=(int)ans;j<tb.length;j++) {
                    tempx.add(tb[j].trim());
                }
                double sum=0.0;
                for(int j=0;j<tempx.size();j++) {
                    sum=sum+1/getdepth(tempx.get(j),ta,tb);
                }
                minsum = sum < minsum ? sum : minsum;
            }
        }
        double sim2 = (max*maxsum-min*minsum)/(max*maxsum);
        return sim2;
    }

    public static double is(String s,List<String> r)
    {
        double cnt=0;
        for(int i=0;i<r.size();i++)
        {
            if(s.equals(r.get(i).trim()))
                cnt++;
        }
        return cnt;
    }

    public static double probc(String name,List<String> data) {
        double freq=is(name,data);
        double pc=freq/Math.sqrt(data.size());
        System.out.println("结果为"+"name："+name+"freq:"+freq+"pc:"+pc);
        if(pc==0)
            pc+=0.0001;//对数不能为0
        return pc;
    }

    public static double IC(String name,List<String> data){
        double pc=probc(name,data);
        return -Math.log(pc);
    }

    public static double Resnik(String icname,String n1,String n2,List<String> d1,List<String> d2) {
        //求LCS的IC
        double len1=d1.size();
        double len2=d2.size();
        double sum=len1+len2;
        //ic1*s1/(s1+s2)+ic2*s2/(s1+s2)加权平均
        double ic1=IC(icname,d1)*(len1/sum);
        double ic2=IC(icname,d2)*(len2/sum);
        return ic1+ic2;
    }

    public static String lca_node(String[] path1,String[] path2){ //寻找最小公共祖先的深度
        //如果开头不一样，则无共同父点
        if(!path1[0].equals(path2[0]))
            return null;
        int p1l = path1.length;
        int p2l = path2.length;
        double ml=p1l < p2l ? p1l : p2l;
        int i=0;
        while(i!=ml){
            if(!path1[i].trim().equals(path2[i].trim())){
                break;
            }
            i=i+1;
        }
        return path1[i].trim();
    }

    public static double Resnik2(String[] a , String[] b,List<String> d1,List<String> d2) {
        //求LCS的IC
        String icname = lca_node(a,b);

        double len1=d1.size();
        double len2=d2.size();
        double sum=len1+len2;
        //ic1*s1/(s1+s2)+ic2*s2/(s1+s2)加权平均
        double ic1=IC(icname,d1)*(len1/sum);
        double ic2=IC(icname,d2)*(len2/sum);
        return ic1+ic2;
    }

    public static double Lin(String c, String n1, String n2, List<String> d1, List<String> d2){
        double ans=0;
        double ic1=IC(n1,d1);
        double ic2=IC(n2,d2);
        double ic=Resnik(c,n1,n2,d1,d2);
        ans=2*Math.log(ic)/(Math.log(ic1)+Math.log(ic2));
        return ans;
    }
}
