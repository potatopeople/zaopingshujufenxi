package cn.adam.bigdata.zhaoping.handlemr.jar;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import cn.adam.bigdata.zhaoping.entity.CSVFormats;
import cn.adam.bigdata.zhaoping.handlemr.jar.work.MapperDemo;
import cn.adam.bigdata.zhaoping.handlemr.jar.work.ReducerDemo;
import cn.adam.bigdata.zhaoping.handlemr.jar.handle.WordHandle;
import cn.adam.bigdata.zhaoping.writable.JobWritable;
import org.apache.hadoop.io.Text;

public class RunMapReduce {
    public static void main(String[] args) {
        conf().runForLocal();
    }

    public static DefaultRunjob conf(){
        DefaultRunjob defaultRunjob = new DefaultRunjob();
        defaultRunjob.addConfClass(WordHandle.class);
//        defaultRunjob.addConfClass(CSVFormats.class);
        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
        defaultRunjob.setRunClass(RunMapReduce.class);
        defaultRunjob.setMapperClass(MapperDemo.class);
        defaultRunjob.setReducerClass(ReducerDemo.class);
        defaultRunjob.setMapOutputKeyClass(Text.class);
        defaultRunjob.setMapOutputValueClass(JobWritable.class);
        defaultRunjob.setInputDir("hdfs:/drsn/rjb/input/");
        defaultRunjob.setInputFileName("ja.csv");
        defaultRunjob.setDelCacheDir(false);

        return defaultRunjob;
    }
}
