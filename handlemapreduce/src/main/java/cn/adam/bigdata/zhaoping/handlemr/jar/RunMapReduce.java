package cn.adam.bigdata.zhaoping.handlemr.jar;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import cn.adam.bigdata.zhaoping.handlemr.jar.fieldhandle.MapReduceDemo;
import cn.adam.bigdata.zhaoping.handlemr.jar.fieldhandle.ReducerDemo;
import cn.adam.bigdata.zhaoping.handlemr.jar.handle.WordHandle;
import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;
import org.apache.hadoop.io.Text;

public class RunMapReduce {
    public static void main(String[] args) {
        conf().runForLocal();
    }

    public static DefaultRunjob conf(){
        DefaultRunjob defaultRunjob = new DefaultRunjob();
        defaultRunjob.addConfClass(new WordHandle());
        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
        defaultRunjob.setRunClass(RunMapReduce.class);
        defaultRunjob.setMapperClass(MapReduceDemo.class);
        defaultRunjob.setReducerClass(ReducerDemo.class);
        defaultRunjob.setMapOutputKeyClass(Text.class);
        defaultRunjob.setMapOutputValueClass(JobWritable.class);
        defaultRunjob.setInputDir("hdfs:/drsn/rjb/input/");
        defaultRunjob.setInputFileName("ja.csv");

        return defaultRunjob;
    }
}
