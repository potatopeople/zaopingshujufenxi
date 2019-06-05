package cn.adam.bigdata.zhaoping.locationget;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import cn.adam.bigdata.zhaoping.entity.CSVFormats;
import cn.adam.bigdata.zhaoping.locationget.work.MapperDemo;
import cn.adam.bigdata.zhaoping.locationget.work.ReducerDemo;
import cn.adam.bigdata.zhaoping.writable.JobWritable;
import org.apache.hadoop.io.Text;

public class RunMapReduce {
    public static void main(String[] args) {
        conf().runForLocal();
    }

    public static DefaultRunjob conf(){
        DefaultRunjob defaultRunjob = new DefaultRunjob();

        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
//        defaultRunjob.addConfClass(CSVFormats.class);
        defaultRunjob.setRunClass(RunMapReduce.class);
        defaultRunjob.setMapperClass(MapperDemo.class);
        defaultRunjob.setReducerClass(ReducerDemo.class);
        defaultRunjob.setMapOutputKeyClass(Text.class);
        defaultRunjob.setMapOutputValueClass(JobWritable.class);
        defaultRunjob.setInputDir("hdfs:/drsn/rjb/input/");
        defaultRunjob.setInputFileName("result_ja.csv");
        defaultRunjob.setOutputFileName("jafinally.csv");
//        defaultRunjob.setDelCacheDir(false);

        return defaultRunjob;
    }
}
