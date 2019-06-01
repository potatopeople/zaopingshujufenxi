package cn.adam.bigdata.zhaoping.handlemr.jar.fieldhandle;

import cn.adam.bigdata.zhaoping.entity.CSVFormats;
import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;
import cn.adam.bigdata.zhaoping.util.Utils;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class MapReduceDemo extends Mapper<LongWritable, Text, Text, JobWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		CSVRecord record = Utils.csvstrToCSVRecord(value.toString(), CSVFormats.BEFOR);
		JobWritable jobWritable = JobWritable.parse(record.toMap());

		context.write(
				new Text(jobWritable.getCompany_name()),
				jobWritable
		);
	}
}
