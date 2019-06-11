package cn.adam.bigdata.zhaoping.analyzedata.work;

import cn.adam.bigdata.zhaoping.analyzedata.entity.CountValue;
import cn.adam.bigdata.zhaoping.analyzedata.entity.XYVValue;
import cn.adam.bigdata.zhaoping.analyzedata.entity.XYValue;
import cn.adam.bigdata.zhaoping.analyzedata.entity.XYZVValue;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultMapper;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultReducer;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AfterJobAnalyze extends DefaultRunjob{
    public static void main(String[] args) {
        DefaultRunjob conf = conf();
        conf.runForLocal();
    }
    public static DefaultRunjob conf(){
        AfterJobAnalyze defaultRunjob = new AfterJobAnalyze();

        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
//        defaultRunjob.addConfClass(CSVFormats.class);
        defaultRunjob.setRunClass(AfterJobAnalyze.class);
        defaultRunjob.setMapperClass(AfterAnalyzeMapper.class);
        defaultRunjob.setReducerClass(AfterAnalyzReducer.class);
        defaultRunjob.setMapOutputKeyClass(Text.class);
        defaultRunjob.setMapOutputValueClass(NullWritable.class);
        defaultRunjob.setInputDir("hdfs:/drsn/rjb/input/");
        defaultRunjob.setInputFileName("jafinally.csv");
        defaultRunjob.setOutputFileName("jtmp.csv");
        return defaultRunjob;
    }

    @Override
    protected void setJob(Job job) throws IOException {
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    }

    static class AfterAnalyzeMapper extends DefaultMapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vs = value.toString().split("\t");
            String k = vs[0]+"\t"+
                    vs[1];
            if (vs[0].equals("info")||vs[0].equals("welfare")){
                k = k +"\t"+ vs[2];
            }
            context.write(new Text(k), value);
        }
    }

    static class AfterAnalyzReducer extends DefaultReducer<Text, Text, Text, NullWritable>{
        private MultipleOutputs<Text, NullWritable> outputs;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] vs = key.toString().split("\t");
            if (vs[0].equals("jobtag")){
                Integer all = null;
                List<CountValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t");
                    int i = Integer.parseInt(split[3]);
                    if (split[2].equals("ALL"))
                        all = i;
                    else {
                        l.add(new CountValue(split[2], i));
                    }
                }

                Collections.sort(l);
                for (int i = 10; i < l.size(); i++) {
                    l.remove(i);
                }

                String json = JSONObject.toJSONString(l);
                outputs.write(new Text(vs[1]+"\t"+json), NullWritable.get(), "jobtag");
                if (all != null) {
                    outputs.write(new Text(vs[1] + "\tALL\t" + all), NullWritable.get(), "jobtag");
                }
            }else if (vs[0].equals("edu")){
                List<CountValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t");
                    int i = Integer.parseInt(split[3]);
                    l.add(new CountValue(split[2], i));
                }

                String json = JSONObject.toJSONString(l);
                outputs.write(new Text(vs[1]+"\t"+json), NullWritable.get(), "edu");
            }else if (vs[0].equals("exp")){
                List<XYValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t");
                    int i = Integer.parseInt(split[3]);
                    int j = Integer.parseInt(split[2]);
                    l.add(new XYValue(j, i));
                }

                String json = JSONObject.toJSONString(l);
                outputs.write(new Text(vs[1]+"\t"+json), NullWritable.get(), "exp");
            }else if (vs[0].equals("sandiantu")){
                List<XYVValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t");
                    int i = Integer.parseInt(split[3]);
                    int j = Integer.parseInt(split[4]);
                    int h = Integer.parseInt(split[2]);
                    l.add(new XYVValue(i, h, j));
                }

                String json = JSONObject.toJSONString(l);
                outputs.write(new Text(vs[1]+"\t"+json), NullWritable.get(), "sandiantu");
            }else if (vs[0].equals("sandiantu2")){
                List<XYZVValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t");
                    int i = Integer.parseInt(split[3]);
                    int j = Integer.parseInt(split[5]);
                    int h = Integer.parseInt(split[2]);
                    l.add(new XYZVValue(i, h, j, split[4]));
                }

                String json = JSONObject.toJSONString(l);
                outputs.write(new Text(vs[1]+"\t"+json), NullWritable.get(), "sandiantu2");
            }else if (vs[0].equals("info")){
                List<CountValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t");
                    int i = Integer.parseInt(split[4]);
                    l.add(new CountValue(split[3], i));
                }

                Collections.sort(l);
                int i = 10;
                if (vs[2].equals("ALL"))
                    i = 20;
                for (; i < l.size(); i++) {
                    l.remove(i);
                }

                String json = JSONObject.toJSONString(l);
                outputs.write(new Text(vs[1]+"\t"+vs[2]+"\t"+json), NullWritable.get(), "info");
            }else if (vs[0].equals("welfare")){
                List<CountValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t");
                    int i = Integer.parseInt(split[4]);
                    l.add(new CountValue(split[3], i));
                }

                Collections.sort(l);
                int i = 10;
                if (vs[2].equals("ALL"))
                    i = 20;
                for (; i < l.size(); i++) {
                    l.remove(i);
                }

                String json = JSONObject.toJSONString(l);
                outputs.write(new Text(vs[1]+"\t"+vs[2]+"\t"+json), NullWritable.get(), "welfare");
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            outputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            outputs.close();
        }
    }
}
