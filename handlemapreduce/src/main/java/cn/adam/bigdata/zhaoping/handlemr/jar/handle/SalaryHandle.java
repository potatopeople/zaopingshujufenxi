package cn.adam.bigdata.zhaoping.handlemr.jar.handle;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SalaryHandle implements Handle<JobWritable> {

    private Pattern p = Pattern.compile(FieldMatch.DOUBLE);
    @Override
    public void handle(JobWritable jobWritable) {
        String salary = jobWritable.getJob_salary();
        if (salary==null||salary.equals("")) {
            jobWritable.setJob_salary(null);
            return;
        }

        String handle = null;
        Matcher matcher = p.matcher(salary);
        Double[] ss = new Double[3];
        for (int i = 0; i < 2 && matcher.find(); i++) {
            ss[i] = Double.parseDouble(matcher.group());
        }

        if (salary.contains("面议")) {
            ss[0] = -1.0;
            ss[1] = -1.0;
            salary = "面议";
        }else {
            if (ss[1] == null){
                if (salary.contains("以上"))
                    ss[1] = 0.0;
                else if (salary.contains("以下") || salary.contains("以内")) {
                    ss[1] = ss[0];
                    ss[0] = 0.0;
                }else
                    ss[1] = ss[0];
            }

            if (salary.contains("千")||salary.contains("k")||salary.contains("K")){
                ss[0] *= 1000;
                ss[1] *= 1000;
            } else if (salary.contains("万")){
                ss[0] *= 10000;
                ss[1] *= 10000;
            }

            if (salary.contains("小时")||salary.contains("天")||ss[1]<1000){
                jobWritable.setJob_salary(null);
                return;
            } else if (salary.contains("年")){
                ss[0] /= 12;
                ss[1] /= 12;
            }else if (!salary.contains("月")){
                String job_edu_require_level = jobWritable.getJob_edu_require_level();
                int edul;
//                if (job_edu_require_level == null){
//                    String job_exp_require_level = jobWritable.getJob_exp_require_level();
//                    if (job_exp_require_level == null)
//                        edul = 0;
//                    else
//                        edul = Integer.parseInt(job_exp_require_level);
//                }else
                    edul = Integer.parseInt(job_edu_require_level);
                if (edul < 4){
//                    System.out.println(jobWritable);
                    if (ss[0] >= 100000){
                        ss[0] /= 12;
                        ss[1] /= 12;
                    }
                }else {
                    if (ss[0] >= 1000000){
                        ss[0] /= 12;
                        ss[1] /= 12;
                    }
                }
            }

            salary = ss[0].longValue()+"-"+ss[1].longValue();
        }

        if (ss[0] == -1)
            ss[2] = ss[1];
        else if (ss[0] == 0)
            ss[2] = ss[1];
        else if (ss[1] == 0)
            ss[2] = ss[0];
        else
            ss[2] = (ss[0]+ss[1])/2;

        jobWritable.setJob_salary(salary);
        jobWritable.setJob_salary_handle(String.valueOf(ss[2].longValue()));
//        System.out.println(jobWritable.getJob_salary_handle());
        jobWritable.setJob_salary_level(getLevel(ss[2]).toString());
    }

    private Integer getLevel(Double i){
        if (i == null)
            return null;
        Integer re = null;
        if (i < 3000)
            re = 0;
        else if (i < 6000)
            re = 1;
        else if (i < 10000)
            re = 2;
        else if (i < 50000)
            re = 3;
        else if (i < 100000)
            re = 4;
        else if (i < 500000)
            re = 5;
        else
            re = 6;
        return re;
    }
}
