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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AfterJobAnalyze extends DefaultRunjob{
    public static void main(String[] args) {
        DefaultRunjob conf = conf();
        conf.runServer();
    }
    public static DefaultRunjob conf(){
        AfterJobAnalyze defaultRunjob = new AfterJobAnalyze();

        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
//        defaultRunjob.addConfClass(CSVFormats.class);
        defaultRunjob.setRunClass(AfterJobAnalyze.class);
        defaultRunjob.setMapperClass(AfterAnalyzeMapper.class);
        defaultRunjob.setReducerClass(AfterAnalyzReducer.class);
        defaultRunjob.setMapOutputKeyClass(Text.class);
        defaultRunjob.setMapOutputValueClass(Text.class);
        defaultRunjob.setInputDir("hdfs:/drsn/rjb/input/");
        defaultRunjob.setInputFileName("jtmp.csv");
        defaultRunjob.setOutputFileName("analyzeresult_job.txt");
//        defaultRunjob.setOutputDir("hdfs:/drsn/rjb/input/analyzeout/");
//        defaultRunjob.setMoveoutfile(false);
        return defaultRunjob;
    }

    @Override
    protected void setJob(Job job) throws IOException {
//        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    }

    static class AfterAnalyzeMapper extends DefaultMapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vs = value.toString().split("\t",-1);
            String k = vs[0]+"\t"+
                    vs[1];
            if (vs[0].equals("info")||vs[0].equals("welfare")){
                k = k +"\t"+ vs[2];
            }
            context.write(new Text(k), new Text(value.toString()));
        }
    }

    static class AfterAnalyzReducer extends DefaultReducer<Text, Text, Text, NullWritable>{
//        private MultipleOutputs<Text, NullWritable> outputs;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] vs = key.toString().split("\t",-1);
            String out;
            if (vs[0].equals("jobtag")){
                Integer all = null;
                List<CountValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t",-1);
                    int i = Integer.parseInt(split[3]);
                    if (split[2].equals("ALL"))
                        all = i;
                    else {
                        l.add(new CountValue(split[2], i));
                    }
                }

                Collections.sort(l);

                l = l.subList(0, 10>l.size()?l.size():10);

                String json = JSONObject.toJSONString(l);

                out = vs[1]+"\trank\tjobtag\t"+json;
//                outputs.write(new Text(out), NullWritable.get(), "jobtag");
                context.write(new Text(out), NullWritable.get());
                if (all != null) {
                    out = vs[1]+"\tcount\tjobtag\t"+all;
//                    outputs.write(new Text(out), NullWritable.get(), "jobtag");
                    context.write(new Text(out), NullWritable.get());
                }
            }else if (vs[0].equals("edu")){
                List<CountValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t",-1);
                    int i = Integer.parseInt(split[3]);
                    l.add(new CountValue(split[2], i));
                }

                String json = JSONObject.toJSONString(l);

                out = vs[1]+"\tradarchart\tedu\t"+json;
//                outputs.write(new Text(out), NullWritable.get(), "edu");
                context.write(new Text(out), NullWritable.get());
            }else if (vs[0].equals("exp")){
                List<XYValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t",-1);
                    int i = Integer.parseInt(split[3]);
                    int j = Integer.parseInt(split[2]);
                    l.add(new XYValue(j, i));
                }

                String json = JSONObject.toJSONString(l);

                out = vs[1]+"\tradarchart\texp\t"+json;
//                outputs.write(new Text(out), NullWritable.get(), "exp");
                context.write(new Text(out), NullWritable.get());
            }else if (vs[0].equals("sandiantu")){
                List<XYVValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t",-1);
                    int i = Integer.parseInt(split[3]);
                    int j = Integer.parseInt(split[4]);
                    int h = Integer.parseInt(split[2]);
                    l.add(new XYVValue(i, h, j));
                }

                String json = JSONObject.toJSONString(l);

                out = vs[1]+"\tscatterplot\tees\t"+json;
//                outputs.write(new Text(out), NullWritable.get(), "sandiantu");
                context.write(new Text(out), NullWritable.get());
            }else if (vs[0].equals("sandiantu2")){
                List<XYZVValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t",-1);
                    int i = Integer.parseInt(split[3]);
                    int j = Integer.parseInt(split[5]);
                    int h = Integer.parseInt(split[2]);
                    l.add(new XYZVValue(i, h, j, split[4]));
                }

                String json = JSONObject.toJSONString(l);

                out = vs[1]+"\tscatterplot\teesj\t"+json;
//                outputs.write(new Text(out), NullWritable.get(), "sandiantu2");
                context.write(new Text(out), NullWritable.get());
            }else if (vs[0].equals("info")){
                List<CountValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t",-1);
                    int i = Integer.parseInt(split[4]);
                    l.add(new CountValue(split[3], i));
                }

                Collections.sort(l);
                int i = 10;
                if (vs[2].equals("ALL"))
                    i = 20;

                l = l.subList(0, i>l.size()?l.size():i);

                String json = JSONObject.toJSONString(l);

                if (vs[2].equals("ALL"))
                    out = vs[1]+"\trank\tinfo\t"+json;
                else
                    out = vs[1]+"\tinfo\t"+vs[2]+"\t"+json;
//                outputs.write(new Text(out), NullWritable.get(), "info");
                context.write(new Text(out), NullWritable.get());
            }else if (vs[0].equals("welfare")){
                List<CountValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] split = t.toString().split("\t",-1);
                    int i = Integer.parseInt(split[4]);
                    l.add(new CountValue(split[3], i));
                }

                Collections.sort(l);
                int i = 10;
                if (vs[2].equals("ALL"))
                    i = 20;

                l = l.subList(0, i>l.size()?l.size():i);

                String json = JSONObject.toJSONString(l);

                if (vs[2].equals("ALL"))
                    out = vs[1]+"\trank\twelfare\t"+json;
                else
                    out = vs[1]+"\twelfare\t"+vs[2]+"\t"+json;
//                outputs.write(new Text(out), NullWritable.get(), "welfare");
                context.write(new Text(out), NullWritable.get());
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
//            outputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
//            outputs.close();
        }
    }
}
