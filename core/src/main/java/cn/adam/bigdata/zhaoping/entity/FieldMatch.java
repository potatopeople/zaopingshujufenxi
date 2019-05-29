package cn.adam.bigdata.zhaoping.entity;

public class   FieldMatch {
    public static final String ALL = "[\\x{4e00}-\\x{9fa5}|0-9|A-z]+";
    public static final String ALLBD = "[\\x{4e00}-\\x{9fa5}|0-9|A-z]+";
    public static final String EMPTY = "\\s+";

    public static final String CFS = "^(战略投资|已上市|其他|IPO上市|A轮|B轮|C轮|D轮(及以上)?" +
            "|上市公司|不需要融资|天使轮|未融资)$";
    public static final String CI = "^(" + ALL + "[,|/]?" + ")+$";
    public static final String CL = null;
    public static final String CN = null;
    public static final String CNA = null;
    public static final String CO = null;
    public static final String CP = "^(公司规模：)?((少于)?\\d+人?(－|-|以上|以下)?|保密){1,2}$";
    public static final String JEDR = "^(学历：?)?((统招)?(MBA|不限|博士|博士后|大专|本科|" +
            "硕士|中专|中技|初中)+(及以下|及以上)?/*)+$";
    public static final String JEXR = "^(经验：?)*(不限|应届(毕业)?生|" +
            "(\\d+[年|-](以上|以下|以内)?)+)$";
    public static final String JI = null;
    public static final String JN = null;
    public static final String JS = "^((\\d+[k|K|万]?-?)+|面议)$";
    public static final String JT = null;
    public static final String JW = null;

    public static final String[] AllFieldMatch = new String[]{
            CFS,CI,CL,CN,CNA,CO,CP,JEDR,JEXR,JI,JN,JS,JT,JW
    };

    public final static String CONF = "adam.conf";
    public final static String JAR = "mapred.jar\tD:\\data\\code\\idea\\zaopingshujufenxi" +
            "\\handlemapreduce\\target\\handlemapreduce-1.0-SNAPSHOT.jar";
    public final static String MP = "mapreduce.app-submission.cross-platform\ttrue";
}
