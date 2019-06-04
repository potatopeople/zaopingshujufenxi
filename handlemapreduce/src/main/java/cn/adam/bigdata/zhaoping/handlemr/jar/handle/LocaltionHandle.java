package cn.adam.bigdata.zhaoping.handlemr.jar.handle;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.writable.JobWritable;

import java.util.regex.Pattern;

public class LocaltionHandle implements Handle<JobWritable> {

    private Pattern p = Pattern.compile(FieldMatch.INT);

    @Override
    public void handle(JobWritable jobWritable) {
        String location = jobWritable.getCompany_location();
        if (location == null)
            return;
        if(location.replaceAll(FieldMatch.ALLNOT, "").equals("")){
            jobWritable.setCompany_location(null);
            return;
        }

        location = location.replaceAll("(^[\\s\\d\\-\"，。、．/.]+|[ \\t]+$)", "")
                .replaceAll("^公司地址： *", "");

        jobWritable.setCompany_location(location);
    }
}
