# 项目说明文档



## 1 使用技术或框架：

	大数据相关：hdfs, mapreduce, hbase, 
	工具，框架相关：ansj（分词），commons-csv（csv数据解析）， fastjson（json数据解析），jsoup（解析地址信息时调用api用到）
	可视化：后端采用的springboot调用hbase api编写的数据接口，前端采用的vue.js和echarts进行可视化
	开发环境：除环境搭建文档中提及到的大数据环境外，还有idea，webstom开发工具，maven，jdk1.8等

## 2 思路和实现流程

### 2.1整体思路

	1), 先编写java程序（项目：yuchuli1）对源数据的空行，多行，多空白字符，进行消除，对字段位置错误（如公司人数）进行修正，
	2), 再编写mapreduce程序（项目：handlemapreduce）对缺少的字段尝试修复，将需要用到的字段处理成易于分析的格式（包括分词处理），根据塞题要求过滤数据，并去除重复数据
	3), 然后编写java程序（项目：locationget）调用腾讯地图高德地图api进行地理位置解析，并编写mapreduce程序将解析后的地址信息融入源数据
	4), 最后，编写mapreduce程序（项目：analyzedata）进行数据分析
	5), 然后编写后端接口，编写前端可视化

### 2.2 分步思路与实现流程
#### 2.2.1 数据收集,预处理（yuchuli1）

```Java

用commons-csv对源数据6个文件分别进行加载，然后去掉每个字段中的空行，换行，多余空白符，再检测公司人数字段是否位置正确，尝试去其他字段找，

处理完成后， 将数据输出到同一个文件（out/ja.csv）中，文件大小在407mb左右，然后将文件上传到hdfs:/drsn/rjb/input/ja.csv中(文件名: ja.csv)

/**
* 如果公司人数字段位置错误，调整人数字段位置
*/
private void cPeopleCorr(List<String> list){
    Pattern p = Pattern.compile(FieldMatch.CP);
    Matcher matcher = p.matcher(list.get(0));
    if (matcher.matches()){
        if ((list.get(1) != null) && list.get(1).equals(list.get(6)))
            list.set(6, list.get(0));
        else {
            String s1 = list.get(1);
            list.set(1, "tmp");
            int i = find(list, s1);
            if (i > 1 && i < 10)
                list.set(i, list.get(0));
            else
                log.warn("人数错误，但未找到其对应的放置位置："+list.get(3));
        }
        list.set(0, "");
    }
}

/**
 * 去除字段中多余空白符（空格，换行，制表符等）
 * @param s 字段字符串
 * @param is 指定是消除所有空白符还是，消除多余空白符
 * @return 返回处理后的字符串
 */
private String work(String s, boolean is) {
    return s.replaceAll(FieldMatch.EMPTY, is ? "" : " ")
        .replaceAll("(^\\s+|\\s+$)", "");
}
```

#### 2.2.2 数据字段处理（handlemapreduce）

```Java
1）.对数据字段进行处理以易于分析，
	比如学历，将中专，中职，职中，职高，中技归为中专一类，
	对无效字符进行清除处理，比如"学历："，“公司规模：”等，
	范围值取平均值，若为0到多少，或者多少以上则取其中那个确定值，
	将学历，经验，工资进行等级划分，
	对职位描述和福利字段进行分词处理（词库来源于网络，根据多个网站收集而来，然后设置白名单分词）
2）.尝试修复不全或不正确字段，
	不全字段或不正确字段，如公司相关信息，可根据同一个公司的数据进行补全，或者，在其他可能出现该信息的字段去查找，进行尝试性的修复，
3）.根据塞题要求过滤数据，并去除重复数据
	将缺少关键数据字段值（数据为空或值为null）的数据过滤掉，关键数据字段为：job_info，job_name。
	无效数据过滤，将job_name中包含“实习”的数据过滤掉。
	去除重复数据：判断依据：公司名company_name，公司地址company_location，公司概况company_overview，职位名job_name， 职位描述job_info，这些字段是否全部相同；
	
	处理后数据约为15万条，原数据19万条左右
	处理后文件afterhandle.csv(472mb左右)

/*
    由于该部分核心代码太多，可查看源代码，handlemapreduce项目下cn.adam.bigdata.zhaoping.handlemr.jar.handle包内的所有用于字段处理的handle
    
    对人数，学历，经验，工资进行了处理，处理成了固定格式，如，人数，经验，工资处理成数值-数值，学历处理成，不限，初中，中专，高中，大专，本科，硕士，博士，其他这几个，
    对同个公司的信息：公司融资阶段，公司行业，公司地址，公司名字，公司描述，公司性质进行对比选择最全的进行同步更新，
    将job_info，job_name数据为空或值为null的数据过滤掉，无效数据过滤，将job_name中包含“实习”的数据过滤掉。
    去除重复数据：判断依据：公司名company_name，公司地址company_location，公司概况company_overview，职位名job_name， 职位描述job_info，这些字段是否全部相同
    对职位描述和职位福利进行分词（白名单，词库收集来源于网络），
*/
```

#### 2.2.3 解析地址信息（locationget）

```Java
编写java程序，调用腾讯地图和高德地图（腾讯地图解析不了时调用）的接口，进行地址信息解析，若location字段缺失为空，则尝试用公司名进行解析；
解析后得到省-市-区-经纬度信息，然后上传到hdfs:/drsn/rjb/input目录下（文件名：lfromcname.txt，lfromlocation.txt）

然后编写mapreduce程序将地址信息和源数据合并，没有地区信息的数据为10%

得到含有地址信息的数据jafinally.csv(479mb左右)

/*
	在reducer的setup方法中加载已上传至hdfs的地址信息文件，然后存在locationMap，from变量用来区分当前的地址信息文件是根据原数据中location字段解析的还是company_name字段解析的，若是根据company_name字段解析的，则还会更新原数据中location的字段为当前地址信息
 */
private String from;
private Map<String, Location> locationMap = new HashMap<>();
@Override
protected void reduce(Text key, Iterable<JobWritable> values,
                          Context context)
    throws IOException, InterruptedException {

    Location location = locationMap.get(key.toString());

    for (JobWritable j : values) {
        if (location != null) {
            if (from != null && from.equals("cname")){
                j.setCompany_location(location.getProvince()+"-"
                                      +location.getCity()+"-"
                                      +location.getDistrict()+"-");
            }
            j.setCompany_location_province(location.getProvince());
            j.setCompany_location_city(location.getCity());
            j.setCompany_location_district(location.getDistrict());
            j.setCompany_location_longitude(location.getLongitude().toString());
            j.setCompany_location_latitude(location.getLatitude().toString());
        }
        String p = j.getCompany_location_province();
        if (p!=null&&p.contains("\uFEFF"))
            j.setCompany_location_province(p.replaceAll("\uFEFF", ""));
        Utils.emptyFieldToNull(j);
        context.write(new Text(j.toString()), NullWritable.get());
    }

}
```

#### 2.2.4 数据分析（analyzedata）


```Java
对于数据分析，我是在地区字段之上进行的各种分析，目前只写了省级，准备第二轮再根据实际效果决定要不要写市级，其中，全国的数据等同于所有数据，没有地区信息的数据我将其依然归于全国，但不归于任何省，所有数据中，没有地区信息的数据为10%
分析数据主要分析了：
1) 赛题要求的：统计各职位所需掌握的前10位技能点。也就是全国的各职位所需掌握的前10位技能点排名。
2) 全国/各省的各职位的前10位职位福利排名。
	说明：上面这两个数据我设计的是用两个图来展示，一个是排名的图，一个是关系图，排名图可以清晰的展示出排名次序，关系图可以很容易的知晓 职位-技能点/职位福利 之间的对应关系。
3) 全国/各省的所有职位的前20技能点排名。
4) 全国/各省的所有职位的前20位职位福利排名。
	说明：上面这两个数据我设计的是用词云来展示。
5) 全国/各省的前10位职位排名。
6) 全国/各省的前10位公司行业排名。
7) 全国/各省的前10位公司性质排名。
	说明：上面这三个数据我设计的是用饼图展示（不包含10位之后的）。
8) 各省的公司数量分布。
9) 各省发布的职位数量分布。
	说明：上面这两个数据我设计的是在地图上展示分布效果。
10) 全国/各省的各学历的职位数量。
11) 全国/各省的各经验要求的职位数量。
	说明：上面这两个数据我设计的是用雷达图展示其分布情况。
12) 全国/各省的学历-经验-工资对应关系。
13) 全国/各省的学历-经验-工资-职位对应关系。
	说明：上面这两个数据我设计的是用散点图展示其分布情况，其中“全国/各省的学历-经验-工资-职位对应关系”是用的带颜色的散点图，颜色区分职位；
	
数据分析处理完后会执行一个导入到hbase的mapreduce，直接将分析结果导入hbase 'job'表中，
表结构：
表主键为“全国”或者各省的名字
count列族（统计数量相关）：company，jobtag
rank列族（排行相关）：jobtag,nature,industry,info,welfare
radarchart列族（雷达图相关）：edu,exp
scatterplot列族（散点图相关）：ees,eesj
info列族（各职位技术词排名）：各职位名
welfare列族（各职位职位福利排名）：各职位名

'job', {NAME => 'count'}, {NAME => 'info'}, {NAME => 'radarchart'}, {NAME => 'rank'}, {NAME => 'scatterplot'}, {NAME => 'welfare'}

/*
	数据分析这块的核心源码也有点多，我就直接阐述大致上的步骤吧，
	数据分析时分成了两块和一个上传至hbase的mapreduce，一块是用来做公司相关的统计，因为数据中存在多职位对应一个公司问题，所以要先对公司做去重处理（也就是analyzedata项目的cn.adam.bigdata.zhaoping.analyzedata.work.BeforConutCompany这个类做的事），然后在此基础上做最一些统计，比如全国/各省公司数量，全国/各省的各个行业的公司数量，全国/各省的各个性质的公司数量（cn.adam.bigdata.zhaoping.analyzedata.work.ConutCompany类），然后再汇总在一起，取排名前十的数据（cn.adam.bigdata.zhaoping.analyzedata.work.AfterConutCompany）
	另外一块就是分析职位相关数据，统计全国/各省职位数和职位排名，全国/各省学历和经验分布，全国/各省学历-经验-工资对应关系，全国/各省学历-经验-工资-职位对应关系，全国/各省各职位和所有职位的技能点/职位福利排名以及对应关系（cn.adam.bigdata.zhaoping.analyzedata.work.JobAnalyze），最后汇总（cn.adam.bigdata.zhaoping.analyzedata.work.AfterAnalyze）
	然后就是用mapreduce将数据结果上传到hbase（cn.adam.bigdata.zhaoping.analyzedata.work.PutToHbase）
*/
```

#### 2.2.5 数据可视化（web）
##### 2.2.5.1 可视化后端
```Java
	采用springboot框架，通过hbase api编写后端数据接口，提供数据分析时的13项数据的接口，返回json数据
	
@RestController
@Getter
@Setter
/*
	提供所有分析数据的后端数据接口
*/
public class DefaultController {
    @Autowired
    private HbaseService hbaseService;
    @GetMapping("/companycount")
    public List<CountValuve> getCompanyCount(){
        return hbaseService.getCount("company");
    }
    @GetMapping("/jobcount")
    public List<CountValuve> getJobCount(){
        return hbaseService.getCount("jobtag");
    }

    @GetMapping("/jobrank")
    public String getJobRank(String location){
        return hbaseService.getString(location, "rank", "jobtag");
    }
    @GetMapping("/naturerank")
    public String getNatureRank(String location){
        return hbaseService.getString(location, "rank", "nature");
    }
    @GetMapping("/industryrank")
    public String getIndustryRank(String location){
        return hbaseService.getString(location, "rank", "industry");
    }
    @GetMapping("/inforank")
    public String getInfoRank(String location){
        return hbaseService.getString(location, "rank", "info");
    }
    @GetMapping("/welfarerank")
    public String getWelfareRank(String location){
        return hbaseService.getString(location, "rank", "welfare");
    }
    @GetMapping("/eduradar")
    public String getEduRadar(String location){
        return hbaseService.getString(location, "radarchart", "edu");
    }
    @GetMapping("/expradar")
    public String getExpRadar(String location){
        return hbaseService.getString(location, "radarchart", "exp");
    }
    @GetMapping("/eesscatter")
    public String getEesScatter(String location){
        return hbaseService.getString(location, "scatterplot", "ees");
    }
    @GetMapping("/eesjscatter")
    public String getEesjScatter(String location){
        return hbaseService.getString(location, "scatterplot", "eesj");
    }


    @GetMapping("/info")
    public List<KValuve> getInfo(String location){
        return hbaseService.getGroup(location, "info");
    }
    @GetMapping("/welfare")
    public List<KValuve> getWelfare(String location){
        return hbaseService.getGroup(location, "welfare");
    }
}
```

##### 2.2.5.2 可视化前端

	采用vue，echarts框架进行前端可视化界面开发
	调用后端提供的数据接口，然后将其用echarts展示出来
	
	界面：
	总体界面：

![1560469784203](img\1560469784203.png)
	1) 各省的公司数量分布。
	2) 各省发布的职位数量分布。
![1560459838972](img\1560459838972.png)
	3) 全国的各职位所需掌握的前10位技能点排名。
	4) 全国/各省的各职位的前10位职位福利排名。
![前10技能点/福利排名](img\1.png)
![前10技能点/福利关系](img\5.png)

	5) 全国/各省的所有职位的前20技能点排名。
![前20技能点排名](img\2.png)

	6) 全国/各省的所有职位的前20位职位福利排名。
	7) 全国/各省的前10位职位排名。
![前10职位排名](img\3.png)

	8) 全国/各省的前10位公司行业排名。
	9) 全国/各省的前10位公司性质排名。
	
	10) 全国/各省的各学历的职位数量。
	11) 全国/各省的各经验要求的职位数量。
![数量分布](img\4.png)

	12) 全国/各省的学历-经验-工资对应关系。
![学历-经验-工资对应关系](img\6.png)

	13) 全国/各省的学历-经验-工资-职位对应关系。
![学历-经验-工资-职位对应关系](img\7.png)



## 3 分析结果及结论（全国）

### 3.1 地图可视化结果

	由地图展示的各地区职位和公司分布情况来看，排名靠前的省份为广东，北京，上海，浙江，江苏，四川；由此可知，取这些地方发展，机遇会大很多；

### 3.2 各职位技能点/职位福利排名

		人工智能职位需要的最主要的技能点：人工智能，大数据，c，python，java，c++等；	职位福利为：五险一金，绩效奖金，带薪年假等
		图像处理职位需要的最主要的技能点：图像处理，c++，c，深度学习，机器学习，python，openCV，TensorFlow等	职位福利为：五险一金，带薪年假，弹性工作等
		图像算法职位需要的最主要的技能点：c++，图像处理，深度学习，c，python，机器学习，openCV，TensorFlow等	职位福利为：五险一金，带薪年假，弹性工作等
		数据分析职位需要的最主要的技能点：数据分析，Excel，MSOffice，ppt，SEO，大数据，SEM等	职位福利为：五险一金，绩效奖金，专业培训等
		数据分析师职位需要的最主要的技能点：数据分析，Excel，python，R，数据挖掘，大数据，SPSS等	职位福利为：五险一金，节日福利，带薪年假等
		数据挖掘职位需要的最主要的技能点：数据挖掘，数据分析，大数据，python，java，hadoop，机器学习，spark等	职位福利为：五险一金，绩效奖金，带薪年假等
		算法职位需要的最主要的技能点：c++，c，python，MATLAB，机器学习，图像处理，深度学习等	职位福利为：五险一金，绩效奖金，定期体检等
	......
	
		求职者可参考这些数据，综合自身条件和需要进行职位的选择；

### 3.3 各职位排名

	由饼图展示结果可看出，所有招聘数据中，职位需求排名靠前的为：数据分析，数据挖掘，人工智能，机器学习，深度学习，算法等

### 3.4 各学历/经验需求分布

	由雷达图可知，所有职位学历要求中，本科要求数量最多，大专其次；
	经验要求2年以上最多，不限经验其次；

### 3.5 学历-经验-工资对应关系

	由散点图可知，一般情况下，学历越高，经验年限越高工资也就越高

其余各省的可以点击地图中对应的位置，所有图表会联动，将数据切换为当前选择省份的数据，再次点击当前选择的省会切换回全国的数据；

### 4 源码

#### 4.1源码包介绍

```
yuchuli1
	cn.adam.bigdata.zhaoping
		handle	数据收集清洗时用到的处理字段的类的包
			Correction.java	字段调整
			Filter.java	字段空白符过滤
		CsvFormat.java	启动该模块的主类，负责读取csv，写出csv

handlemapreduce
	cn.adam.bigdata.zhaoping.handlemr.jar
		handle	数据字段处理时用到的各个字段相应处理的类的包
			CompanyCompletionGroupHandle.java	同步同个公司的与公司相关的字段
			EduHandle.java	处理学历字段
			ExpHandle.java	处理经验字段
			FinancingHandle.java	处理融资阶段字段
			IndustryHandle.java	处理行业字段
			it.txt	基数词词库
			LocaltionHandle.java	处理地址信息字段
			PeopleHandle.java	处理人数字段
			SalaryHandle.java	处理工资字段
			welfare.txt	职位福利词库
			WordHandle.java	处理技术词和职位福利的分词
			
locationget
	cn.adam.bigdata.zhaoping.locationget
		entity
			Location.java	地理位置Bean
		work
			MapperDemo.java	融合位置信息的Mapper
			ReducerDemo.java	融合位置信息的Reducer
		MainClass.java	调用地图api解析位置信息
		RunJob.java	开发环境远程提交jar到服务器执行的job启动类
		RunMapReduce.java	服务器执行的job启动类
	resources	资源文件夹
		location.txt	原数据中提出来的地址信息，用于解析地址
		companyname.txt	原数据中提出来的公司名字信息，用于解析地址
		
analyzedata
	cn.adam.bigdata.zhaoping.analyzedata
		entity	用到的一些实体类
			CountValue.java
			XYValue.java
			XYVValue.java
			XYZVValue.java
        work
			AfterConutCompany.java	汇总公司相关的统计数据
			AfterJobAnalyze.java	汇总职位相关的统计数据
			BeforConutCompany.java	统计公司信息前线对公司去重
			ConutCompany.java	统计公司信息
			JobAnalyze.java	统计职位相关信息
			PutToHbase.java	将数据导入到hbase
        RunJob.java	启动mapreduce的类
        
web
	cn.adam.bigdata.zhaoping.web
		controller
			DefaultController.java	提供数据接口的控制器
		entity	用到的实体类
			CountValuve.java
			KValuve.java
		service
			HbaseService.java	操作hbase api的service层
		util
			HbaseUtils.java 操作hbase api的类
		WebApplication.java	启动springboot的类
```



#### 4.2 可执行文件介绍

由于我提供了可执行的包，所以就不介绍源码执行了，

大数据环境：我的大数据环境为一个namenode节点3个datanode节点，分别为master，slave1，slave2，slave3，

zookeeper在slave1，2，3这三台机器上，hmaster在master机器上，

1. 先将6份最初的数据文件放在yuchuli1包同目录下，执行run-yuchuli1.bat，会在当前目录生成一个out文件夹，里边有一个ja.csv文件，

2. 将ja.csv文件上传至hdfs中，路径为：hdfs:/drsn/rjb/input/ja.csv

3. hadoop运行handlemapreduce包    “hadoop -jar handlemapreduce-1.0-SNAPSHOT.jar”

4. 将我提供的两个地址信息文件（lfromcname.txt， lfromlocation.txt）上传至hdfs，文件名不变，和ja.cav同目录；

5. hadoop运行locationget包    “hadoop -jar locationget-1.0-SNAPSHOT.jar”

6. hadoop运行analyzedata包    “hadoop -jar analyzedata-1.0-SNAPSHOT.jar”    注意：会提示输入hbase使用的zookeeper的服务器地址（hbase.zookeeper.quorum参数的值），运行前需要创建好hbase表

   `create 'job', {NAME => 'count'}, {NAME => 'info'}, {NAME => 'radarchart'}, {NAME => 'rank'}, {NAME => 'scatterplot'}, {NAME => 'welfare'}`

7. 修改



