package cn.adam.bigdata.zhaoping.defaultdemo;

import cn.adam.bigdata.zhaoping.basic.HaveConfFileTemp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class DefaultMapper<KI, VI, KO, VO> extends Mapper<KI, VI, KO, VO> {
	@Override
	protected abstract void map(KI key, VI value, Context context) throws IOException, InterruptedException;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration configuration = context.getConfiguration();
		HaveConfFileTemp.set(configuration);
	}
}
