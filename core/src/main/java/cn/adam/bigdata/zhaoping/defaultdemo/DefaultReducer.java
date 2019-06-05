package cn.adam.bigdata.zhaoping.defaultdemo;

import cn.adam.bigdata.zhaoping.basic.HaveConfFileTemp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public abstract class DefaultReducer<KI, VI, KO, VO> extends Reducer<KI, VI, KO, VO>{
	@Override
	protected abstract void reduce(KI key, Iterable<VI> values, Context context) throws IOException, InterruptedException ;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration configuration = context.getConfiguration();
		HaveConfFileTemp.set(configuration);
	}
}
