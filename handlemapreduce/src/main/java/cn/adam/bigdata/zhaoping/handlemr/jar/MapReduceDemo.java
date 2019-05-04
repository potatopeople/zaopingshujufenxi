package cn.adam.bigdata.zhaoping.handlemr.jar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class MapReduceDemo extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String[] strings = StringUtils.split(value.toString(), '，');

		if(strings.length > 14){
			context.write(new Text("err"), new IntWritable(1));
			return;
		}
		int i = 0;
		for (String string : strings) {
			context.write(new Text(i + "-----------"+string), new IntWritable(1));
		}
	}
}
