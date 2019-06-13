package cn.adam.bigdata.zhaoping.analyzedata;

import cn.adam.bigdata.zhaoping.analyzedata.work.*;

public class RunJob {
    public static void main(String[] args) {
        JobAnalyze.main(new String[0]);
        AfterJobAnalyze.main(new String[0]);
        BeforConutCompany.main(new String[0]);
        ConutCompany.main(new String[0]);
        AfterConutCompany.main(new String[0]);
        PutToHbase.main(new String[0]);
    }
}
