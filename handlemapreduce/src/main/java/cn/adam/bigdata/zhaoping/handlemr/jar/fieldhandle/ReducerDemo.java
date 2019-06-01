package cn.adam.bigdata.zhaoping.handlemr.jar.fieldhandle;

import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerDemo extends Reducer<Text, JobWritable, Text, NullWritable>{
	@Override
	protected void reduce(Text key, Iterable<JobWritable> values,
						  Context context)
			throws IOException, InterruptedException {

	}
}
