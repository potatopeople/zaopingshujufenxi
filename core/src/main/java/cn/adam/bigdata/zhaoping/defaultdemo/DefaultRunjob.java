package cn.adam.bigdata.zhaoping.defaultdemo;

import cn.adam.bigdata.zhaoping.basic.HaveConfFile;
import cn.adam.bigdata.zhaoping.basic.HaveConfFileTemp;
import cn.adam.bigdata.zhaoping.entity.CSVFormats;
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
import java.util.*;

@Slf4j
public class DefaultRunjob {

	public final static String MP = "mapreduce.app-submission.cross-platform";
	public final static String HAVECONFDIR = "adam_haveconffiledir";
	public final static String JAR = "mapred.jar";
	public final static String OUT = "tempout/";
	public final static String OUTPREFIX = "result_";
	@Getter@Setter
	protected String jobName = "test"+ Math.random();
	protected Map<String, String> confmap;
	@Getter@Setter
	protected String cacheDir = "hdfs:/conf/";
	@Getter@Setter
	protected boolean delCacheDir = true;
	@Getter@Setter
	protected boolean isServer = true;
	protected Set<Class<? extends HaveConfFile>> confClass = new HashSet<>();

	@NonNull@Getter@Setter
	protected Class<?> runClass;
	@NonNull@Getter@Setter
	protected Class<? extends Mapper> mapperClass;
	@NonNull@Getter@Setter
	protected Class<? extends Reducer> reducerClass;
	@NonNull@Getter@Setter
	protected Class<?> mapOutputKeyClass;
	@NonNull@Getter@Setter
	protected Class<?> mapOutputValueClass;

	@NonNull@Getter@Setter
	protected String inputDir;
	@Getter@Setter
	protected String inputFileName = "";
	@Getter@Setter
	protected String outputDir;
	@Getter@Setter
	protected String outputFileName;

	public DefaultRunjob(){
		confmap = new HashMap<>();
	}
	public void runForRemote(@NonNull String jarPath){
		this.addConfClass(CSVFormats.class);
		this.checkConf();
		this.addConf(MP, "true");
		this.addConf(JAR, jarPath);
		run();
	}
	public void runForLocal(){
		this.checkConf();
		this.confClass.clear();
		run();
	}

	protected void setJob(Job job) throws IOException {
	}

	protected void run() {
		Configuration configuration = new Configuration();

		for (Map.Entry<String, String> e : this.confmap.entrySet())
			configuration.set(e.getKey(), e.getValue());

		configuration.set("dfs.permissions","false");
		Job job;
		
		try {
			FileSystem fs = FileSystem.get(configuration);

			HaveConfFileTemp.CONF = configuration;
			if (this.isServer)
				configuration.set(HAVECONFDIR, cacheDir);
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

			if (!this.inputDir.endsWith("/"))
				this.inputDir+="/";

			Path in = new Path(this.inputDir+this.inputFileName);

			if (this.outputDir == null)
				this.outputDir = this.inputDir+OUT;
			else if (!this.outputDir.endsWith("/"))
				this.outputDir +="/";
			Path out = new Path(this.outputDir);

			if (fs.exists(out)) {
				fs.delete(out, true);
			}

			FileInputFormat.addInputPath(job, in);
			FileOutputFormat.setOutputPath(job, out);
			setJob(job);
			boolean f = job.waitForCompletion(true);
			if (f) {
				FileStatus[] fileStatuses = fs.listStatus(out);
				for (FileStatus fileStatus : fileStatuses) {
					Path outfilepath = fileStatus.getPath();
					if (outfilepath.getName().startsWith("part")) {
						StringBuilder sb = new StringBuilder();
						if (this.outputDir.equals(this.inputDir+OUT))
							sb.append(this.inputDir);
						else
							sb.append(this.outputDir);
						if (this.outputFileName == null)
							sb.append(OUTPREFIX + this.inputFileName);
						else
							sb.append(this.outputFileName);
						Path op = new Path(sb.toString());
						if (fs.exists(op))
							fs.delete(op, true);
						fs.rename(outfilepath, op);
					}
				}
				log.info("sucsses");
			}
			if (delCacheDir&&this.confClass.size()>0)
				fs.delete(new Path(cacheDir), true);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			log.error("mapreduce执行出错！", e);
		}

	}

	protected void updateConfFileToHDFS(FileSystem fs, Configuration configuration){
//		configuration.set(HAVECONFDIR, cacheDir);
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

		for (Class<? extends HaveConfFile > o : confClass) {
			try {
				HaveConfFile h = (HaveConfFile) o.newInstance();
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

	protected void checkConf(){
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

	public Set<Class<? extends HaveConfFile>> getConfClass() {
		return confClass;
	}
	public void setConfClass(Set<Class<? extends HaveConfFile>> confClass) {
		this.confClass = confClass;
	}
	public boolean addConfClass(Class<? extends HaveConfFile> h) {
		return confClass.add(h);
	}
	public boolean addConfClass(Collection<? extends Class<? extends HaveConfFile>> h) {
		return confClass.addAll(h);
	}
	public boolean removeConfClass(Class<? extends HaveConfFile> h) {
		return this.confClass.remove(h);
	}
}
