package cn.adam.bigdata.zhaoping.basic;

import cn.adam.bigdata.zhaoping.defaultdemo.DefaultRunjob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public abstract class HaveConfFileTemp implements HaveConfFile {
    protected static Path confPath = null;
    public static Configuration CONF = null;
    public static void setConfDir(Path path){
        confPath = path;
    }

    public static Path getConfPath() {
        return confPath;
    }

    public static void set(Configuration configuration) {
        CONF = configuration;
        String dir = configuration.get(DefaultRunjob.HAVECONFDIR);
        if (dir == null || dir.equals("")) {
            return;
        }
        setConfDir(new Path(dir));
    }
}
