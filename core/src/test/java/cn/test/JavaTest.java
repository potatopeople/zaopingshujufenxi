package cn.test;

import cn.adam.bigdata.zhaoping.basic.HaveConfFileTemp;
import cn.adam.bigdata.zhaoping.entity.CSVFormats;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;
import java.net.URL;

public class JavaTest {
    @Test
    public void test1(){
        HaveConfFileTemp.CONF = new Configuration();
        System.out.println(CSVFormats.getBEFOR());
        System.out.println(CSVFormats.getAFTER());
        System.out.println(CSVFormats.getAFTERGETLOCATION());
    }
}
