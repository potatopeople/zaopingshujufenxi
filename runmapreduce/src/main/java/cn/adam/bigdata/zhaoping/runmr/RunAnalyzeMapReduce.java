package cn.adam.bigdata.zhaoping.runmr;

import cn.adam.bigdata.zhaoping.analyzedata.work.*;
import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;

public class RunAnalyzeMapReduce {

    static DefaultRunjob conf;
    public static void main(String[] args) {
//        System.setProperty("hadoop.home.dir", "D:\\program\\greensoft\\hadoop-2.7.5");

        conf = JobAnalyze.conf();
        run();

        conf = AfterJobAnalyze.conf();
        run();

        conf = BeforConutCompany.conf();
        run();

        conf = ConutCompany.conf();
        run();

        conf = AfterConutCompany.conf();
        run();


        conf = PutToHbase.conf();
        run();

    }
    static void run(){
        conf.runForRemote("D:\\data\\code\\idea\\zaopingshujufenxi" +
                "\\analyzedata\\target\\analyzedata-1.0-SNAPSHOT.jar");
    }
}
