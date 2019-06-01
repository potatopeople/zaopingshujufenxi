package cn.adam.bigdata.zhaoping.handlemr.jar.fieldhandle;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.handlemr.jar.handle.CompanyCompletionGroupHandle;
import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;
import cn.adam.bigdata.zhaoping.util.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ReducerDemo extends Reducer<Text, JobWritable, Text, NullWritable>{
	private Handle<Iterable<JobWritable>>[] handles = new Handle[]{
			new CompanyCompletionGroupHandle()
	};
	@Override
	protected void reduce(Text key, Iterable<JobWritable> values,
						  Context context)
			throws IOException, InterruptedException {

        List<JobWritable> valuelist = new ArrayList<>();

        Iterator<JobWritable> iterator = values.iterator();
//        while (iterator.hasNext()){
        for (JobWritable j : values) {
            valuelist.add(Utils.copyFieldToObject(j, new JobWritable()));
        }

		for (Handle<Iterable<JobWritable>> h : handles) {
			handles[0].handle(valuelist);
		}
//
        Set<JobWritable> valueset = new HashSet<>(valuelist);

		for (JobWritable j : valueset) {
            Utils.emptyFieldToNull(j);
			context.write(new Text(j.toString()), NullWritable.get());
		}

	}
}
