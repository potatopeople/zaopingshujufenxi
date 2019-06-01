package cn.adam.bigdata.zhaoping.handlemr.jar.handle;

import cn.adam.bigdata.zhaoping.basic.FieldHandleTemp;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EduFieldHandle extends FieldHandleTemp<JobWritable> {

    private Pattern p = Pattern.compile(FieldMatch.EDU);
    private Pattern p2 = Pattern.compile(FieldMatch.EDU2);
    private String[] edus = {"不限","初中","中专","高中","大专","本科","硕士","博士","其他"};
    @Override
    public void handle(JobWritable jobWritable) {
        String edu = jobWritable.getJob_edu_require();
        if (edu == null||edu.equals("")||edu.startsWith("招")) {
            Matcher matcher = p.matcher(jobWritable.getJob_info());
            if (matcher.find()) {
                jobWritable.setJob_edu_require(matcher.group());
                edu = jobWritable.getJob_edu_require();
            } else {
                jobWritable.setJob_edu_require(null);
                return;
            }
        }

        String str = null;
        String handle = null;

        Matcher matcher = p2.matcher(edu);
        if (matcher.find())
            str = matcher.group();

        if (edu.contains("不限")||edu.contains("以下"))
            handle = "不限";
        else if (edu.contains("初中"))
            handle = "初中";
        else if (edu.contains("中专")||edu.contains("中技")
                ||edu.contains("中职")||edu.contains("职高"))
            handle = "中专";
        else if (edu.contains("高中"))
            handle = "高中";
        else if (edu.contains("高职")||edu.contains("大专"))
            handle = "大专";
        else if (edu.contains("本科"))
            handle = "本科";
        else if (edu.contains("硕士")||edu.contains("MBA"))
            handle = "硕士";
        else if (edu.contains("博士"))
            handle = "博士";
        else if (edu.contains("其他"))
            handle = "其他";

        jobWritable.setJob_edu_require(str);
        jobWritable.setJob_edu_require_handle(handle);
        jobWritable.setJob_edu_require_level(getLevel(handle).toString());
    }

    private Integer getLevel(String e){
        int i;
        for (i = 0; i < edus.length; i++) {
            if (edus[i].equals(e))
                break;
        }
        return i;
    }
}
