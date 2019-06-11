package cn.adam.bigdata.zhaoping.analyzedata.work;

import cn.adam.bigdata.zhaoping.analyzedata.entity.CountValue;
import cn.adam.bigdata.zhaoping.analyzedata.entity.XYVValue;
import cn.adam.bigdata.zhaoping.analyzedata.entity.XYValue;
import cn.adam.bigdata.zhaoping.analyzedata.entity.XYZVValue;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultMapper;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultReducer;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import cn.adam.bigdata.zhaoping.entity.CSVFormats;
import cn.adam.bigdata.zhaoping.util.Utils;
import cn.adam.bigdata.zhaoping.writable.JobWritable;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.IntWritable;
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

public class JobAnalyze extends DefaultRunjob{
    public static void main(String[] args) {
        DefaultRunjob conf = conf();
        conf.runForLocal();
    }
    public static DefaultRunjob conf(){
        JobAnalyze defaultRunjob = new JobAnalyze();

        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
//        defaultRunjob.addConfClass(CSVFormats.class);
        defaultRunjob.setRunClass(JobAnalyze.class);
        defaultRunjob.setMapperClass(JobAnalyzeMapper.class);
        defaultRunjob.setReducerClass(JobAnalyzeReducer.class);
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

    static class JobAnalyzeMapper extends DefaultMapper<LongWritable, Text, Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            CSVRecord record = Utils.csvstrToCSVRecord(value.toString(), CSVFormats.getAFTERGETLOCATION());
            JobWritable jobWritable = JobWritable.parse(record.toMap());

            if ("company_financing_stage".equals(jobWritable.getCompany_financing_stage()))
                return;

            Utils.emptyFieldToNull(jobWritable);

            String jobtag = jobWritable.getJob_tag();
            String province = jobWritable.getCompany_location_province();
            String[] info = null;
            if (jobWritable.getJob_info_words() != null)
                info = jobWritable.getJob_info_words().split(",");
            else
                info = new String[0];
            String[] welfare = null;
            if (jobWritable.getJob_welfare_words() != null)
                welfare = jobWritable.getJob_welfare_words().split(",");
            else
                welfare = new String[0];

            //职位排名
//            context.write(new Text("jobtag\tALL"), new IntWritable(1));
            context.write(new Text("jobtag\t全国\t" + jobtag), new IntWritable(1));
            context.write(new Text("jobtag\t全国\tALL"), new IntWritable(1));
            if (province != null){
                context.write(new Text("jobtag\t" + province +"\tALL"), new IntWritable(1));
                context.write(new Text("jobtag\t" + province +"\t" + jobtag), new IntWritable(1));
            }

            //雷达图（学历）
            String edu = jobWritable.getJob_edu_require_level();
            if (edu != null) {
                context.write(new Text("edu\t全国\t" + edu), new IntWritable(1));
                if (province != null){
                    context.write(new Text("edu\t" + province + "\t" + edu), new IntWritable(1));
                }
            }

            //雷达图（经验）
            String exp = jobWritable.getJob_exp_require_level();
            if (exp != null) {
                context.write(new Text("exp\t全国\t" + exp), new IntWritable(1));
                if (province != null) {
                    context.write(new Text("exp\t" + province + "\t" + exp), new IntWritable(1));
                }
            }

            //散点图
            String salary = jobWritable.getJob_salary_handle();
            String exp2 = jobWritable.getJob_exp_require_handle();
            if (salary.equals("-1")) salary = null;
            if (edu != null&& exp != null&& salary != null){
                try {
                    int e2 = Integer.parseInt(exp2);
                    if (e2 > 10) e2 = 10;
                    Double sar = Double.parseDouble(salary);
                    context.write(new Text("sandiantu\t全国\t"+edu+"\t"+exp), new IntWritable(sar.intValue()));
                    context.write(new Text("sandiantu2\t全国\t"+edu+"\t"+e2+"\t"+jobtag), new IntWritable(sar.intValue()));
                    if (province != null) {
                        context.write(new Text("sandiantu\t" + province + "\t" + edu + "\t" + exp), new IntWritable(sar.intValue()));
                        context.write(new Text("sandiantu2\t" + province + "\t" + edu + "\t" + e2 + "\t" + jobtag), new IntWritable(sar.intValue()));
                    }
                }catch (Exception e){}
            }

            //技术词
            for (int i = 0; i < info.length; i++) {
//                context.write(new Text("info\tALL\t"+info[i]), new IntWritable(1));
                context.write(new Text("info\t全国\tALL\t"+info[i]), new IntWritable(1));
                context.write(new Text("info\t全国\t" + jobtag + "\t"+info[i]), new IntWritable(1));
                if (province != null){
                    context.write(new Text("info\t" + province +"\tALL\t"+info[i]), new IntWritable(1));
                    context.write(new Text("info\t" + province +"\t" + jobtag + "\t"+info[i]), new IntWritable(1));
                }
            }

            //福利
            for (int i = 0; i < welfare.length; i++) {
//                context.write(new Text("welfare\tALL\t"+welfare[i]), new IntWritable(1));
                context.write(new Text("welfare\t全国\tALL\t"+welfare[i]), new IntWritable(1));
                context.write(new Text("welfare\t全国\t" + jobtag + "\t"+welfare[i]), new IntWritable(1));
                if (province != null){
                    context.write(new Text("welfare\t" + province +"\tALL\t"+welfare[i]), new IntWritable(1));
                    context.write(new Text("welfare\t" + province +"\t" + jobtag + "\t"+welfare[i]), new IntWritable(1));
                }
            }
        }
    }

    static class JobAnalyzeReducer extends DefaultReducer<Text, IntWritable, Text, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            long sum = 0;
            if (key.toString().startsWith("sandiantu")){
                for (IntWritable t : values) {
                    sum+=t.get();
                    i++;
                }
                sum = sum/i;
            }else {
                for (IntWritable t : values)
                    i++;

                sum = i;
            }
            context.write(new Text(key.toString()+"\t"+sum), NullWritable.get());
        }
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
