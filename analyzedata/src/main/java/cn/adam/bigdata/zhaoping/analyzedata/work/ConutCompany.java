package cn.adam.bigdata.zhaoping.analyzedata.work;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultMapper;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultReducer;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ConutCompany extends DefaultRunjob {
    public static void main(String[] args) {
        DefaultRunjob conf = conf();
        conf.runForLocal();
    }
    public static DefaultRunjob conf(){
        ConutCompany defaultRunjob = new ConutCompany();

        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
//        defaultRunjob.addConfClass(CSVFormats.class);
        defaultRunjob.setRunClass(ConutCompany.class);
        defaultRunjob.setMapperClass(ConutCompanyMapper.class);
        defaultRunjob.setReducerClass(ConutCompanyReducer.class);
        defaultRunjob.setMapOutputKeyClass(Text.class);
        defaultRunjob.setMapOutputValueClass(IntWritable.class);
        defaultRunjob.setInputDir("hdfs:/drsn/rjb/input/");
        defaultRunjob.setInputFileName("jtmp.csv");
        defaultRunjob.setOutputFileName("jtmp.csv");
        return defaultRunjob;
    }

    static class ConutCompanyMapper extends DefaultMapper<LongWritable, Text, Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vs = value.toString().split("\t", -1);

            context.write(new Text("company\t全国"), new IntWritable(1));
            if (vs[2]!=null && !vs[2].equals(""))
                context.write(new Text("company\t"+vs[2]), new IntWritable(1));

            if (vs[5] != null&&!vs[5].equals("")) {
                context.write(new Text("nature\t全国\t" + vs[5]), new IntWritable(1));
                if (vs[2] != null && !vs[2].equals(""))
                    context.write(new Text("nature\t" + vs[2] + "\t" + vs[5]), new IntWritable(1));
            }

            if (vs[1] != null&&!vs[1].equals("")) {
                String[] split = vs[1].split(",",-1);
                for (int i = 0; i < split.length; i++) {
                    context.write(new Text("industry\t全国\t" + split[i]), new IntWritable(1));
                    if (vs[2] != null && !vs[2].equals(""))
                        context.write(new Text("industry\t" + vs[2] + "\t" + split[i]), new IntWritable(1));
                }
            }
        }
    }

    static class ConutCompanyReducer extends DefaultReducer<Text, IntWritable, Text, NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (IntWritable t : values)
                i++;

            context.write(new Text(key.toString()+"\t"+i), NullWritable.get());
        }
    }
}
