package cn.adam.bigdata.zhaoping.handlemr.jar.handle;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.writable.JobWritable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IndustryHandle implements Handle<JobWritable> {
    @Override
    public void handle(JobWritable jobWritable) {
        String company_industry = jobWritable.getCompany_industry();
        company_industry.replaceAll("\\s", "")
                .replaceAll("、", "/")
                .replaceAll("（", "(")
                .replaceAll("）", ")")
                .replaceAll("，", ",");

        jobWritable.setCompany_industry(company_industry);
    }
}
