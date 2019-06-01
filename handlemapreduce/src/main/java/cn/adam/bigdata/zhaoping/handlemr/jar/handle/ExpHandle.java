package cn.adam.bigdata.zhaoping.handlemr.jar.handle;

import cn.adam.bigdata.zhaoping.basic.HandleTemp;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExpHandle extends HandleTemp<JobWritable> {

    private Pattern p = Pattern.compile(FieldMatch.INT);
    @Override
    public void handle(JobWritable jobWritable) {
        String exp = jobWritable.getJob_exp_require();

        if (exp==null||"".equals(exp))
            return;
        Matcher matcher = p.matcher(exp);
        Integer[] es = new Integer[2];
        for (int i = 0; i < 2 && matcher.find(); i++) {
            es[i] = Integer.parseInt(matcher.group());
        }

        String r = null;
        if (es[1] == null){
            if (es[0] == null){
                if (exp.contains("在读"))
                    r = "在读";
                else if (exp.contains("应届"))
                    r = "应届生";
                else
                    r = "不限";
            }else {
                if (exp.contains("以上")){
                    es[1] = 0;
                }else if (exp.contains("以下")||exp.contains("以内")){
                    r = "不限";
                }else {
                    es[1] = es[0];
                }
            }
        }

        if (r != null){
            es[0] = 0;
            es[1] = 0;
        }else {
            if (es[0] == 0&&es[1] == 0)
                r = "不限";
            else
                r = es[0]+"-"+es[1];
        }

        jobWritable.setJob_exp_require(r);
        jobWritable.setJob_exp_require_handle(es[0].toString());
        jobWritable.setJob_exp_require_level(getLevel(es[0]).toString());
    }

    private Integer getLevel(Integer i){
        if (i == null)
            return null;
        Integer re = null;
        if (i == 0)
            re = 0;
        else if (i == 1)
            re = 1;
        else if (i <= 3)
            re = 2;
        else if (i <= 5)
            re = 3;
        else if (i <= 7)
            re = 4;
        else if (i <= 10)
            re = 5;
        else
            re = 6;
        return re;
    }
}
