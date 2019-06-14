package cn.adam.bigdata.zhaoping.analyzedata.work;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultMapper;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultReducer;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import cn.adam.bigdata.zhaoping.entity.CSVFormats;
import cn.adam.bigdata.zhaoping.util.Utils;
import cn.adam.bigdata.zhaoping.writable.JobWritable;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class JobAnalyze extends DefaultRunjob{
    public static void main(String[] args) {
        DefaultRunjob conf = conf();
        conf.runServer();
    }
    public static DefaultRunjob conf(){
        JobAnalyze defaultRunjob = new JobAnalyze();

        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
//        defaultRunjob.addConfClass(CSVFormats.class);
        defaultRunjob.setRunClass(JobAnalyze.class);
        defaultRunjob.setMapperClass(JobAnalyzeMapper.class);
        defaultRunjob.setReducerClass(JobAnalyzeReducer.class);
        defaultRunjob.setMapOutputKeyClass(Text.class);
        defaultRunjob.setMapOutputValueClass(IntWritable.class);
        defaultRunjob.setInputDir("hdfs:/drsn/rjb/input/");
        defaultRunjob.setInputFileName("jafinally.csv");
        defaultRunjob.setOutputFileName("jtmp.csv");
        return defaultRunjob;
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
                info = jobWritable.getJob_info_words().split(",",-1);
            else
                info = new String[0];
            String[] welfare = null;
            if (jobWritable.getJob_welfare_words() != null)
                welfare = jobWritable.getJob_welfare_words().split(",",-1);
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
            if (salary!=null&&salary.equals("-1")) salary = null;
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
}
