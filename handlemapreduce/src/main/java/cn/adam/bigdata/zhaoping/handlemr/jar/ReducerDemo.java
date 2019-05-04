package cn.adam.bigdata.zhaoping.handlemr.jar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerDemo extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable intWritable : arg1) {
			sum += intWritable.get();
		}
		arg2.write(arg0, new IntWritable(sum));
	}
}
