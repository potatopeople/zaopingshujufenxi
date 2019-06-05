package cn.adam.bigdata.zhaoping.analyzedata;

import cn.adam.bigdata.zhaoping.analyzedata.work.MapperDemo;
import cn.adam.bigdata.zhaoping.analyzedata.work.ReduceDemo;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import org.apache.hadoop.io.Text;

public class RunMapreduce {
    public static void main(String[] args) {
        conf().runForLocal();
    }

    public static DefaultRunjob conf(){
        DefaultRunjob defaultRunjob = new DefaultRunjob();

        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
        defaultRunjob.setRunClass(RunMapreduce.class);
        defaultRunjob.setMapperClass(MapperDemo.class);
        defaultRunjob.setReducerClass(ReduceDemo.class);
        defaultRunjob.setMapOutputKeyClass(Text.class);
        defaultRunjob.setMapOutputValueClass(Text.class);
        defaultRunjob.setInputDir("hdfs:/drsn/rjb/input/");
        defaultRunjob.setInputFileName("jafinally.csv");

        return defaultRunjob;
    }
}
