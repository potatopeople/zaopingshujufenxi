package cn.adam.bigdata.zhaoping.defaultdemo;

import cn.adam.bigdata.zhaoping.basic.HaveConfFile;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.util.Utils;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
public class DefaultRunjob {

	public final static String MP = "mapreduce.app-submission.cross-platform";
	public final static String JAR = "mapred.jar";
	public final static String OUT = "tempout/";
	public final static String OUTPREFIX = "result_";
	@Getter@Setter
	private String jobName = "test"+ Math.random();
	private Map<String, String> confmap;
	@Getter@Setter
	private String cacheDir = "hdfs:/conf/";
	@Getter@Setter
	private boolean delCacheDir = true;
	private Set<HaveConfFile> confClass = new HashSet<>();

	@NonNull@Getter@Setter
	private Class<?> runClass;
	@NonNull@Getter@Setter
	private Class<? extends Mapper> mapperClass;
	@NonNull@Getter@Setter
	private Class<? extends Reducer> reducerClass;
	@NonNull@Getter@Setter
	private Class<?> mapOutputKeyClass;
	@NonNull@Getter@Setter
	private Class<?> mapOutputValueClass;

	@NonNull@Getter@Setter
	private String inputDir;
	@Getter@Setter
	private String inputFileName = "";
	@Getter@Setter
	private String outputDir;
	@Getter@Setter
	private String outputFileName = "";

	public DefaultRunjob(){
		confmap = new HashMap<>();
	}
	public void runForServer(@NonNull String jarPath){
		this.checkConf();
		this.addConf(MP, "true");
		this.addConf(JAR, jarPath);
		run();
	}
	public void runForLocal(){
		this.checkConf();
		run();
	}
	private void run() {
		Configuration configuration = new Configuration();

		for (Map.Entry<String, String> e : this.confmap.entrySet())
			configuration.set(e.getKey(), e.getValue());

		configuration.set("dfs.permissions","false");
		Job job;
		
		try {
			FileSystem fs = FileSystem.get(configuration);

//			HaveConfFileTemp.CONF = configuration;
			if (this.confClass.size() > 0) {
				log.info("处理配置文件！");
				updateConfFileToHDFS(fs, configuration);
			}
			job = Job.getInstance(configuration);
			job.setJarByClass(this.runClass);
			job.setJobName(this.jobName);
			job.setMapperClass(this.mapperClass);
			job.setReducerClass(this.reducerClass);
			job.setMapOutputKeyClass(this.mapOutputKeyClass);
			job.setMapOutputValueClass(this.mapOutputValueClass);

			FileInputFormat.addInputPath(job,
					new Path(this.inputDir+this.inputFileName));
			if (this.outputDir == null) {
				if (!this.inputDir.endsWith("/"))
					inputDir+="/";
				this.outputDir = this.inputDir+OUT;
			}
			Path out = new Path(this.outputDir);

			if (fs.exists(out)) {
				fs.delete(out, true);
			}
			FileOutputFormat.setOutputPath(job, out);
			boolean f = job.waitForCompletion(true);
			if (f) {
				FileStatus[] fileStatuses = fs.listStatus(out);
				for (FileStatus fileStatus : fileStatuses) {
					Path outfilepath = fileStatus.getPath();
					if (outfilepath.getName().startsWith("part")) {
						StringBuilder sb = new StringBuilder();
						if (this.outputDir.startsWith(this.inputDir+OUT))
							sb.append(this.inputDir);
						else
							sb.append(this.outputDir);
						if (this.outputFileName == null)
							sb.append(OUTPREFIX + this.inputFileName);
						else
							sb.append(this.outputFileName);
						fs.rename(outfilepath,new Path(sb.toString()));
					}
				}
				log.info("sucsses");
			}
			if (delCacheDir)
				fs.delete(new Path(cacheDir), true);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			log.error("mapreduce执行出错！", e);
		}

	}

	private void updateConfFileToHDFS(FileSystem fs, Configuration configuration){
		configuration.set(FieldMatch.HAVECONFDIR, cacheDir);
//		HaveConfFileTemp.setConfDir(new Path(cacheDir));
		Path hdfsCachePath = new Path(cacheDir);

		try {
			if (fs.exists(hdfsCachePath))
				fs.delete(hdfsCachePath, true);
			fs.mkdirs(hdfsCachePath);
		} catch (IOException e) {
			log.error("初始化conf的hdfs文件夹失败！",e);
			throw new RuntimeException(e);
		}

		for (HaveConfFile h : confClass) {
			try {
				String[] confFile = h.getConfFile();
				for (String f : confFile) {
					System.out.println(f);
					fs.copyFromLocalFile(new Path("file:"+f), hdfsCachePath);
				}
			} catch (Exception e) {
				log.error("conf文件处理失败！",e);
				throw new RuntimeException(e);
			}
		}

	}

	private void checkConf(){
		if (Utils.haveNull(this.runClass, this.mapperClass, this.reducerClass,
				this.mapOutputKeyClass, this.mapOutputValueClass, this.inputDir))
			throw new RuntimeException("配置不全，请补全！");
	}

	public String addConf(String k, String v) {
		return this.confmap.put(k, v);
	}
	public String removeConfs(String k) {
		return this.confmap.remove(k);
	}

	public boolean addConfClass(HaveConfFile h) {
		return confClass.add(h);
	}
	public boolean removeConfClass(HaveConfFile h) {
		return this.confClass.remove(h);
	}
}