package cn.adam.bigdata.zhaoping.entity;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;

@Slf4j
public class CSVFormats {
    public static CSVFormat FIRSTRECORDASHEADER = CSVFormat.DEFAULT.withFirstRecordAsHeader();
    public static CSVFormat DEFAULT = CSVFormat.DEFAULT;
    public static CSVFormat  BEFOR = CSVFormat.DEFAULT.withHeader(
            "company_financing_stage", "company_industry", "company_location",
            "company_name", "company_nature", "company_overview", "company_people",
            "job_edu_require", "job_exp_require", "job_info", "job_name", "job_salary",
            "job_tag", "job_welfare"
    );
    public static CSVFormat AFTER = CSVFormat.DEFAULT.withHeader(
            "company_financing_stage","company_industry","company_location",
            "company_name", "company_nature", "company_overview", "company_people",
            "company_people_handle", "company_people_level", "job_edu_require",
            "job_edu_require_handle", "job_edu_require_level", "job_exp_require",
            "job_exp_require_handle", "job_exp_require_level", "job_info", "job_info_words",
            "job_name", "job_salary", "job_salary_handle", "job_salary_level", "job_tag",
            "job_welfare", "job_welfare_words"
    );

}
