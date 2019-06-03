package cn.adam.bigdata.zhaoping.runmr;

import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.handlemr.jar.fieldhandle.Runjob;

public class RunMapReduce {

    private final static String CONF = FieldMatch.CONF;
    private final static String JAR = FieldMatch.JAR;
    private final static String MP = FieldMatch.MP;
    private final static String localdir = FieldMatch.INOUTDIR+
            "\tF:\\rjb\\input\\ja.csv,F:\\rjb\\output";
    private final static String serverdir = FieldMatch.INOUTDIR+
            "\thdfs:/drsn/rjb/input/,hdfs:/drsn/rjb/output,ja.csv";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\program\\greensoft\\hadoop-2.7.5");

//        conf().runForServer(JAR);
        cn.adam.bigdata.zhaoping.handlemr.jar.RunMapReduce.conf().runForServer("D:\\data\\code\\idea\\zaopingshujufenxi" +
                "\\handlemapreduce\\target\\handlemapreduce-1.0-SNAPSHOT.jar");
//        runLoacahost(); //本地运行

//        runServer(); //服务器运行
    }

    /**
     * 本地运行
     */
    public static void runLoacahost() {
        Runjob.main(new String[]{CONF,localdir});
    }

    /**
     * 服务器运行
     * 需要加入配置文件
     */
    public static void runServer() {
        String haveconf = FieldMatch.HAVECONFCLASS+
                "\tcn.adam.bigdata.zhaoping.handlemr.jar.handle.WordHandle";
        String confdir = FieldMatch.HAVECONFDIR+"\thdfs:/drsn/rjb/conf/";
        Runjob.main(new String[]{CONF,JAR,MP,haveconf,confdir,serverdir});
    }

    public static String getCONF() {
        return CONF;
    }
}
