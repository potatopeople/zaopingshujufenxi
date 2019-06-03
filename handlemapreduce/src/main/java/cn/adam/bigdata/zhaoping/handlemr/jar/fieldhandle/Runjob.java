package cn.adam.bigdata.zhaoping.handlemr.jar.fieldhandle;

import cn.adam.bigdata.zhaoping.basic.HaveConfFile;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

@Slf4j
public class Runjob {
	public static void main(String[] args) {
		Configuration configuration = new Configuration();

		boolean is = false;
		for (String s : args){
			if (FieldMatch.CONF.equals(s)){
				is = true;
				continue;
			}
			if (is){
				String[] ss = s.split("\t");
				configuration.set(ss[0], ss[1]);
			}
		}
		configuration.set("dfs.permissions","false");
		Job job;
		
		try {
			FileSystem fs = FileSystem.get(configuration);

			String haveconf = configuration.get(FieldMatch.HAVECONFCLASS);
			if (haveconf != null && !haveconf.equals("")) {
				System.out.println(2333);
				updateConfFileToHDFS(fs, configuration);
			}
			job = Job.getInstance(configuration);
			job.setJarByClass(Runjob.class);
			job.setJobName("test2");
			job.setMapperClass(MapReduceDemo.class);
			job.setReducerClass(ReducerDemo.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(JobWritable.class);

			String[] inout = configuration.get(FieldMatch.INOUTDIR).split(",");
			FileInputFormat.addInputPath(job, new Path(inout[0]+inout[2]));
			Path out = new Path(inout[1]);

//			FileInputFormat.addInputPath(job, new Path("hdfs:/drsn/rjb/input/ja.csv"));
//			Path out = new Path("hdfs:/drsn/rjb/output");

//			FileInputFormat.addInputPath(job, new Path("F:\\rjb\\input\\ja.csv"));
//			Path out = new Path("F:\\rjb\\output");
			if (fs.exists(out)) {
				fs.delete(out, true);
			}
			FileOutputFormat.setOutputPath(job, out);
			
			boolean f = job.waitForCompletion(true);
			if (f) {
				FileStatus[] fileStatuses = fs.listStatus(out);
				for (FileStatus fileStatus : fileStatuses) {
					Path outfilepath = fileStatus.getPath();
					if (outfilepath.getName().startsWith("part"))
						fs.rename(outfilepath,
								new Path(inout[0]+"jobfinally.csv"));
				}
				System.out.println("sucssec");
			}
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			log.error("mapreduce执行出错！", e);
		}

	}

	private static void updateConfFileToHDFS(FileSystem fs, Configuration configuration){
		String[] confclass = configuration.get(FieldMatch.HAVECONFCLASS).split(",");
		String hdfs = configuration.get(FieldMatch.HAVECONFDIR);
		Path hdfsPath = new Path(hdfs);

		try {
			if (fs.exists(hdfsPath))
				fs.delete(hdfsPath, true);
			fs.mkdirs(hdfsPath);
		} catch (IOException e) {
			log.error("初始化conf的hdfs文件夹失败！",e);
			throw new RuntimeException(e);
		}

		for (String s : confclass) {
			try {
				Class<?> clazz = Class.forName(s);
				HaveConfFile o = (HaveConfFile)clazz.newInstance();
				String[] confFile = o.getConfFile();
				for (String f : confFile) {
					System.out.println(f);
					fs.copyFromLocalFile(new Path("file:"+f), hdfsPath);
				}
			} catch (Exception e) {
				log.error("conf文件处理失败！",e);
				throw new RuntimeException(e);
			}
		}

	}
}
