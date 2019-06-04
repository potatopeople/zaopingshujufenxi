package cn.adam.bigdata.zhaoping.handlemr.jar.work;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultMapper;
import cn.adam.bigdata.zhaoping.entity.CSVFormats;
import cn.adam.bigdata.zhaoping.handlemr.jar.handle.*;
import cn.adam.bigdata.zhaoping.writable.JobWritable;
import cn.adam.bigdata.zhaoping.util.Utils;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MapperDemo extends DefaultMapper<LongWritable, Text, Text, JobWritable> {

	private Handle<JobWritable>[] handles;

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		CSVRecord record = Utils.csvstrToCSVRecord(value.toString(), CSVFormats.getBEFOR());
		JobWritable jobWritable = JobWritable.parse(record.toMap());

		if ("company_financing_stage".equals(jobWritable.getCompany_financing_stage()))
			return;
		for (Handle<JobWritable> h : handles) {
			h.handle(jobWritable);
		}

		context.write(
				new Text(jobWritable.getCompany_name()),
				jobWritable
		);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		handles = new Handle[]{
				new PeopleHandle(),
				new EduHandle(),
				new ExpHandle(),
				new SalaryHandle(),
				new WordHandle()
		};
	}
}
