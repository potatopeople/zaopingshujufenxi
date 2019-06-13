package cn.adam.bigdata.zhaoping.runmr;

public class RunHandleMapReduce {

    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir", "D:\\program\\greensoft\\hadoop-2.7.5");

        cn.adam.bigdata.zhaoping.handlemr.jar.RunMapReduce.conf().runForRemote("D:\\data\\code\\idea\\zaopingshujufenxi" +
                "\\handlemapreduce\\target\\handlemapreduce-1.0-SNAPSHOT.jar");
//        DefaultRunjob conf = cn.adam.bigdata.zhaoping.handlemr.jar.RunHandleMapReduce.conf();
//        conf.setInputDir("file:/F:/rjb/input/");
//        conf.runForLocal();
    }
}
