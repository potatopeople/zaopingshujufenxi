package cn.adam.bigdata.zhaoping.analyzedata.work;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultMapper;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.Scanner;

public class PutToHbase extends DefaultRunjob {
    public static void main(String[] args) {
        DefaultRunjob conf = conf();
        conf.runServer();
    }
    public static DefaultRunjob conf(){
        PutToHbase defaultRunjob = new PutToHbase();

        defaultRunjob.setCacheDir("hdfs:/drsn/rjb/conf/");
//        defaultRunjob.addConfClass(CSVFormats.class);
        Scanner sc = new Scanner(System.in);
        System.out.println("请输入 hbase.zookeeper.quorum 的值：");
        defaultRunjob.addConf("hbase.zookeeper.quorum", sc.nextLine());
        defaultRunjob.addConf(TableOutputFormat.OUTPUT_TABLE, "job");
        defaultRunjob.addConf("dfs.socket.timeout", "180000");
        defaultRunjob.setRunClass(PutToHbase.class);
        defaultRunjob.setMapperClass(PutToHbaseMapper.class);
        defaultRunjob.setReducerClass(PutToHbaseReducer.class);
        defaultRunjob.setMapOutputKeyClass(Text.class);
        defaultRunjob.setMapOutputValueClass(Text.class);
        defaultRunjob.setInputDir("hdfs:/drsn/rjb/input/");
        defaultRunjob.setInputFileName("analyzeresult_*");
        defaultRunjob.setMoveoutfile(false);
//        defaultRunjob.setOutputFileName("jtmp.csv");
        return defaultRunjob;
    }

    @Override
    protected void setJob(Job job) throws IOException {
        job.setInputFormatClass(TextInputFormat.class);
        // 不再设置输出路径，而是设置输出格式类型
        job.setOutputFormatClass(TableOutputFormat.class);
    }

    static class PutToHbaseMapper extends DefaultMapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] vs = value.toString().split("\t", -1);
            context.write(new Text(vs[0]), new Text(value.toString()));
        }
    }

    static class PutToHbaseReducer extends TableReducer<Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Put put = new Put(Bytes.toBytes(key.toString()));

            for (Text t : values) {
                String[] ts = t.toString().split("\t", -1);
                put.addColumn(Bytes.toBytes(ts[1]),
                        Bytes.toBytes(ts[2]),
                        Bytes.toBytes(ts[3])
                );
            }

            context.write(NullWritable.get(), put);
        }
    }
}
