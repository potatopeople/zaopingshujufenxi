package cn.adam.bigdata.zhaoping.locationget.work;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultMapper;
import cn.adam.bigdata.zhaoping.entity.CSVFormats;
import cn.adam.bigdata.zhaoping.util.Utils;
import cn.adam.bigdata.zhaoping.writable.JobWritable;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MapperDemo extends DefaultMapper<LongWritable, Text, Text, JobWritable> {

	private String from;
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		CSVRecord record = Utils.csvstrToCSVRecord(value.toString(), CSVFormats.getAFTERGETLOCATION());
		JobWritable jobWritable = JobWritable.parse(record.toMap());

		if ("company_financing_stage".equals(jobWritable.getCompany_financing_stage()))
			return;

		String k;
		if (from != null && from.equals("cname"))
			k = jobWritable.getCompany_name();
		else
			k = jobWritable.getCompany_location();
		context.write(
				new Text(k),
				jobWritable
		);
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		from = context.getConfiguration().get(ReducerDemo.LOCATIONDROM);
	}
}
