package cn.adam.bigdata.zhaoping.handlemr.jar;

import cn.adam.bigdata.zhaoping.handlemr.RunMapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Runjob {
	public static void main(String[] args) {
		Configuration configuration = new Configuration();

		boolean is = false;
		for (String s : args){
			if (RunMapReduce.getCONF().equals(s)){
				is = true;
				continue;
			}
			if (is){
				String[] ss = s.split("\t");
				configuration.set(ss[0], ss[1]);
			}
		}
		Job job;
		
		try {
			FileSystem fs = FileSystem.get(configuration);
			
			job = Job.getInstance(configuration);
			job.setJarByClass(Runjob.class);
			job.setJobName("test2");
			job.setMapperClass(MapReduceDemo.class);
			job.setReducerClass(ReducerDemo.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path("hdfs:/drsn/test/input/test_text.txt"));
			Path out = new Path("hdfs:/drsn/test/output");
			if (fs.exists(out)) {
				fs.delete(out, true);
			}
			FileOutputFormat.setOutputPath(job, out);
			
			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("sucssec");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
