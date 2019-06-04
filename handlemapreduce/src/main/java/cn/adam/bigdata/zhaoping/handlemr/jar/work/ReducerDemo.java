package cn.adam.bigdata.zhaoping.handlemr.jar.work;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.basic.HaveConfFileTemp;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultReducer;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import cn.adam.bigdata.zhaoping.handlemr.jar.handle.CompanyCompletionGroupHandle;
import cn.adam.bigdata.zhaoping.handlemr.jar.handle.LocaltionHandle;
import cn.adam.bigdata.zhaoping.writable.JobWritable;
import cn.adam.bigdata.zhaoping.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class ReducerDemo extends DefaultReducer<Text, JobWritable, Text, NullWritable>{
	private Handle<Iterable<JobWritable>>[] grouphandles = new Handle[]{
			new CompanyCompletionGroupHandle()
	};
	private Handle<JobWritable>[] fieldhandles = new Handle[]{
			new LocaltionHandle()
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

		for (Handle<Iterable<JobWritable>> h : grouphandles) {
			h.handle(valuelist);
		}
//
        Set<JobWritable> valueset = new HashSet<>(valuelist);

		for (JobWritable j : valueset) {
			for (Handle<JobWritable> h : fieldhandles)
				h.handle(j);
            Utils.emptyFieldToNull(j);
			context.write(new Text(j.toString()), NullWritable.get());
		}

	}

}
