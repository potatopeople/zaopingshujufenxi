package cn.adam.bigdata.zhaoping.handlemr.jar.handle;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.entity.FieldMatch;
import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PeopleHandle implements Handle<JobWritable> {

    private Pattern p = Pattern.compile(FieldMatch.INT);

    @Override
    public void handle(JobWritable jobWritable) {
        String people = jobWritable.getCompany_people();
        if (people == null||"".equals(people) || people.contains("保密")) {
            jobWritable.setCompany_people(null);
            return;
        }
        Matcher matcher = p.matcher(people);
        Integer[] ps = new Integer[3];
        for (int i = 0; i < 2 && matcher.find(); i++) {
            ps[i] = Integer.parseInt(matcher.group());
        }

        if (ps[1] == null){
            if (people.contains("以上"))
                ps[1] = 0;
            else if (people.contains("以下") || people.contains("少于")) {
                ps[1] = ps[0];
                ps[0] = 0;
            }else
                ps[1] = ps[0];
        }

        if (ps[0] == 0)
            ps[2] = ps[1];
        else if (ps[1] == 0)
            ps[2] = ps[0];
        else
            ps[2] = (ps[0]+ps[1])/2;

        jobWritable.setCompany_people(ps[0]+"-"+ps[1]);
        jobWritable.setCompany_people_handle(ps[2].toString());
        jobWritable.setCompany_people_level(getLevel(ps[2]).toString());
    }

    private Integer getLevel(Integer i){
        if (i == null)
            return null;
        Integer re = null;
        if (i < 100)
            re = 0;
        else if (i < 500)
            re = 1;
        else if (i < 1000)
            re = 2;
        else if (i < 5000)
            re = 3;
        else if (i < 10000)
            re = 4;
        else
            re = 5;
        return re;
    }
}
