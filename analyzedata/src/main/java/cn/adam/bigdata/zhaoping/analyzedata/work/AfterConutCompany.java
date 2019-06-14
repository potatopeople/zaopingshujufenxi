package cn.adam.bigdata.zhaoping.analyzedata.work;

import cn.adam.bigdata.zhaoping.analyzedata.entity.CountValue;
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

public class AfterConutCompany extends DefaultRunjob {
    public static void main(String[] args) {
        DefaultRunjob conf = conf();
        conf.runServer();
    }
    public static DefaultRunjob conf(){
        AfterConutCompany defaultRunjob = new AfterConutCompany();

        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
//        defaultRunjob.addConfClass(CSVFormats.class);
        defaultRunjob.setRunClass(AfterConutCompany.class);
        defaultRunjob.setMapperClass(AfterConutCompanyMapper.class);
        defaultRunjob.setReducerClass(AfterConutCompanyReducer.class);
        defaultRunjob.setMapOutputKeyClass(Text.class);
        defaultRunjob.setMapOutputValueClass(Text.class);
        defaultRunjob.setInputDir("hdfs:/drsn/rjb/input/");
        defaultRunjob.setInputFileName("jtmp.csv");
        defaultRunjob.setOutputFileName("analyzeresult_countcompany.txt");
//        defaultRunjob.setOutputDir("hdfs:/drsn/rjb/input/companyanalyzeout/");
//        defaultRunjob.setMoveoutfile(false);
        return defaultRunjob;
    }

    @Override
    protected void setJob(Job job) throws IOException {
//        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    }

    static class AfterConutCompanyMapper extends DefaultMapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] vs = value.toString().split("\t",-1);
            String k = vs[0]+"\t"+
                    vs[1];
            context.write(new Text(k), new Text(value.toString()));
        }
    }

    static class AfterConutCompanyReducer extends DefaultReducer<Text, Text, Text, NullWritable>{

//        private MultipleOutputs<Text, NullWritable> outputs;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] vs = key.toString().split("\t",-1);
            String out;
            if (vs[0].equals("company")){
                for (Text t : values) {
                    String[] ts = t.toString().split("\t",-1);
                    int i = Integer.parseInt(ts[2]);

                    out = ts[1] + "\tcount\tcompany\t"+i;
                    context.write(new Text(out), NullWritable.get());
//                    outputs.write(new Text(out), NullWritable.get(), "company");
                }
            }else if (vs[0].equals("nature")){
                List<CountValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] ts = t.toString().split("\t",-1);
                    int i = Integer.parseInt(ts[3]);
                    l.add(new CountValue(ts[2], i));
                }

                Collections.sort(l);

                l = l.subList(0, 10>l.size()?l.size():10);

                String json = JSONObject.toJSONString(l);

                out = vs[1] + "\trank\tnature\t"+json;
                context.write(new Text(out), NullWritable.get());
//                outputs.write(new Text(out), NullWritable.get(), "nature");
            }else if (vs[0].equals("industry")){
                List<CountValue> l = new ArrayList<>();
                for (Text t : values) {
                    String[] ts = t.toString().split("\t",-1);
                    int i = Integer.parseInt(ts[3]);
                    l.add(new CountValue(ts[2], i));
                }

                Collections.sort(l);

                l = l.subList(0, 10>l.size()?l.size():10);

                String json = JSONObject.toJSONString(l);

                out = vs[1] + "\trank\tindustry\t"+json;
                context.write(new Text(out), NullWritable.get());
//                outputs.write(new Text(out), NullWritable.get(), "industry");
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
