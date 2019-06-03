package cn.adam.bigdata.zhaoping.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public abstract class HaveConfFileTemp implements HaveConfFile {
    protected static Path confPath = null;
    public static Configuration CONF = null;
    public static void setConfDir(Path path){
        confPath = path;
    }
}
