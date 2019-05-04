package cn.adam.bigdata.zhaoping.handlemr;

import cn.adam.bigdata.zhaoping.handlemr.jar.Runjob;

public class RunMapReduce {

    private final static String CONF = "adam.conf";
    private final static String JAR = "mapred.jar\tD:\\data\\myprogram\\ideaPro\\git\\zaopingshujufenxi" +
            "\\handlemapreduce\\target\\handlemapreduce-1.0-SNAPSHOT.jar";
    private final static String MP = "mapreduce.app-submission.cross-platform\ttrue";

    public static void main(String[] args) {
        Runjob.main(new String[]{CONF,JAR,MP});
    }

    public static String getCONF() {
        return CONF;
    }
}
