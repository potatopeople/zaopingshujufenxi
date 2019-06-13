package cn.adam.bigdata.zhaoping.locationget;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;

public class RunJob {
    public static void main(String[] args) {
        DefaultRunjob conf = RunMapReduce.conf();
        conf.runForLocal();
    }
}
