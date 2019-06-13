package cn.adam.bigdata.zhaoping.handlemr.jar;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import cn.adam.bigdata.zhaoping.handlemr.jar.handle.WordHandle;
import cn.adam.bigdata.zhaoping.handlemr.jar.work.MapperDemo;
import cn.adam.bigdata.zhaoping.handlemr.jar.work.ReducerDemo;
import cn.adam.bigdata.zhaoping.writable.JobWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;

public class RunMapReduce extends DefaultRunjob{
    public static void main(String[] args) {
        conf().runForLocal();
    }

    public static DefaultRunjob conf(){
        DefaultRunjob defaultRunjob = new RunMapReduce();
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
        defaultRunjob.setOutputFileName("afterhandle.csv");
        defaultRunjob.setDelCacheDir(false);

        return defaultRunjob;
    }

    @Override
    protected void setConfiguration(Configuration conf) {
        conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, GzipCodec.class, CompressionCodec.class);
    }
}
