package cn.adam.bigdata.zhaoping.handlemr.jar.handle;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.writable.JobWritable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FinancingHandle implements Handle<JobWritable> {
    private Pattern p = Pattern.compile("(A轮|B轮|C轮|D轮|D轮及以上|IPO上市|上市公司|不需要融资|天使轮|已上市|战略投资|未融资)");
    @Override
    public void handle(JobWritable jobWritable) {
        String company_financing_stage = jobWritable.getCompany_financing_stage();
        if (company_financing_stage == null||company_financing_stage.equals("")) {
            String s = jobWritable.getCompany_name()+jobWritable.getCompany_overview();
            Matcher matcher = p.matcher(s);
            if (matcher.find())
                company_financing_stage = matcher.group();
        }
        if(company_financing_stage!=null&&company_financing_stage.equals("上市公司"))
            company_financing_stage = "已上市";

        jobWritable.setCompany_financing_stage(company_financing_stage);
    }
}
