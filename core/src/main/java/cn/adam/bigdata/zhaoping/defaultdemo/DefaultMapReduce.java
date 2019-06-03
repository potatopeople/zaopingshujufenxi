package cn.adam.bigdata.zhaoping.defaultdemo;

import cn.adam.bigdata.zhaoping.basic.HaveConfFileTemp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public abstract class DefaultMapReduce<KI, VI, KO, VO> extends Mapper<KI, VI, KO, VO> {
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration configuration = context.getConfiguration();
		HaveConfFileTemp.CONF = configuration;
		String dir = configuration.get(DefaultRunjob.HAVECONFDIR);
		if (dir == null || dir.equals("")) {
			return;
		}
		HaveConfFileTemp.setConfDir(new Path(dir));
	}
}
