package cn.adam.bigdata.zhaoping.handlemr.jar.fieldhandle;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.entity.CSVFormats;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.handlemr.jar.handle.*;
import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;
import cn.adam.bigdata.zhaoping.basic.HaveConfFileTemp;
import cn.adam.bigdata.zhaoping.util.Utils;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapReduceDemo extends Mapper<LongWritable, Text, Text, JobWritable> {

	private Handle<JobWritable>[] handles = new Handle[]{
			new PeopleHandle(),
			new EduHandle(),
			new ExpHandle(),
			new SalaryHandle(),
			new WordHandle()
	};

	public MapReduceDemo(){
	}
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		CSVRecord record = Utils.csvstrToCSVRecord(value.toString(), CSVFormats.BEFOR);
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
		Configuration configuration = context.getConfiguration();
		HaveConfFileTemp.CONF = configuration;
		String dir = configuration.get(FieldMatch.HAVECONFDIR);
		if (dir == null || dir.equals("")) {
			return;
		}
		HaveConfFileTemp.setConfDir(new Path(dir));
	}
}
