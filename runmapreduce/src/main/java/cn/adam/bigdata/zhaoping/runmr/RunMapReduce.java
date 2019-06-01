package cn.adam.bigdata.zhaoping.runmr;

import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.handlemr.jar.fieldhandle.Runjob;

public class RunMapReduce {

    private final static String CONF = FieldMatch.CONF;
    private final static String JAR = FieldMatch.JAR;
    private final static String MP = FieldMatch.MP;

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\program\\greensoft\\hadoop-2.7.5");
//        Runjob.main(new String[]{CONF,JAR,MP});

        Runjob.main(new String[]{});
    }

    public static String getCONF() {
        return CONF;
    }
}
