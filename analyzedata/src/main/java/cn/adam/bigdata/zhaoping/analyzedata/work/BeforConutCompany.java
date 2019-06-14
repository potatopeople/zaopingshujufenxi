package cn.adam.bigdata.zhaoping.analyzedata.work;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultMapper;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultReducer;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import cn.adam.bigdata.zhaoping.entity.CSVFormats;
import cn.adam.bigdata.zhaoping.util.Utils;
import cn.adam.bigdata.zhaoping.writable.JobWritable;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class BeforConutCompany extends DefaultRunjob {
    public static void main(String[] args) {
        DefaultRunjob conf = conf();
        conf.runServer();
    }
    public static DefaultRunjob conf(){
        BeforConutCompany defaultRunjob = new BeforConutCompany();

        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
//        defaultRunjob.addConfClass(CSVFormats.class);
        defaultRunjob.setRunClass(BeforConutCompany.class);
        defaultRunjob.setMapperClass(BeforConutCompanyMapper.class);
        defaultRunjob.setReducerClass(BeforConutCompanyReducer.class);
        defaultRunjob.setMapOutputKeyClass(Text.class);
        defaultRunjob.setMapOutputValueClass(Text.class);
        defaultRunjob.setInputDir("hdfs:/drsn/rjb/input/");
        defaultRunjob.setInputFileName("jafinally.csv");
        defaultRunjob.setOutputFileName("jtmp.csv");
        return defaultRunjob;
    }

    static class BeforConutCompanyMapper extends DefaultMapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            CSVRecord record = Utils.csvstrToCSVRecord(value.toString(), CSVFormats.getAFTERGETLOCATION());
            JobWritable jobWritable = JobWritable.parse(record.toMap());

            if ("company_financing_stage".equals(jobWritable.getCompany_financing_stage()))
                return;

            StringBuilder sb = new StringBuilder();
            sb.append(jobWritable.getCompany_financing_stage()).append("\t")
                    .append(jobWritable.getCompany_industry()).append("\t")
                    .append(jobWritable.getCompany_location_province()).append("\t")
                    .append(jobWritable.getCompany_location_city()).append("\t")
                    .append(jobWritable.getCompany_name()).append("\t")
                    .append(jobWritable.getCompany_nature()).append("\t")
                    .append(jobWritable.getCompany_people_handle());
            context.write(new Text(jobWritable.getCompany_name()), new Text(sb.toString()));
        }
    }

    static class BeforConutCompanyReducer extends DefaultReducer<Text, Text, Text, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(), NullWritable.get());
        }
    }
}
