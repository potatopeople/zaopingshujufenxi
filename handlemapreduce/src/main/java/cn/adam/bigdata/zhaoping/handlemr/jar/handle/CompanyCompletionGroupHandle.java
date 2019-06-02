package cn.adam.bigdata.zhaoping.handlemr.jar.handle;

import cn.adam.bigdata.zhaoping.basic.Handle;
import cn.adam.bigdata.zhaoping.handlemr.jar.writable.JobWritable;

public class CompanyCompletionGroupHandle implements Handle<Iterable<JobWritable>> {
    @Override
    public void handle(Iterable<JobWritable> jobWritables) {
        JobWritable job = new JobWritable();
        for (JobWritable j : jobWritables) {
            if (j.getCompany_financing_stage() != null) {
                if (job.getCompany_financing_stage() == null || job.getCompany_financing_stage().equals("")
                        || job.getCompany_financing_stage().length() < j.getCompany_financing_stage().length()
                )
                    job.setCompany_financing_stage(j.getCompany_financing_stage());
            }

            if (j.getCompany_industry() != null) {
                if (job.getCompany_industry() == null
                        || job.getCompany_industry().equals("")
                        || job.getCompany_industry().length() < j.getCompany_industry().length()
                )
                    job.setCompany_industry(j.getCompany_industry());
            }

            if (j.getCompany_location() != null) {
                if (job.getCompany_location() == null
                        || job.getCompany_location().equals("")
                        || job.getCompany_location().length() < j.getCompany_location().length()
                )
                    job.setCompany_location(j.getCompany_location());
            }

            if (j.getCompany_nature() != null) {
                if (job.getCompany_nature() == null
                        || job.getCompany_nature().equals("")
                        || job.getCompany_nature().length() < j.getCompany_nature().length()
                )
                    job.setCompany_nature(j.getCompany_nature());
            }

            if (j.getCompany_overview() != null) {
                if (job.getCompany_overview() == null
                        || job.getCompany_overview().equals("")
                        || job.getCompany_overview().length() < j.getCompany_overview().length()
                )
                    job.setCompany_overview(j.getCompany_overview());
            }
        }
//        System.out.println(job);
        for (JobWritable j : jobWritables) {
            j.setCompany_financing_stage(job.getCompany_financing_stage());
            j.setCompany_industry(job.getCompany_industry());
            j.setCompany_location(job.getCompany_location());
            j.setCompany_nature(job.getCompany_nature());
            j.setCompany_overview(job.getCompany_overview());
        }
    }
}
