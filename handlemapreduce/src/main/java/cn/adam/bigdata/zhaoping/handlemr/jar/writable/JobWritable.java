package cn.adam.bigdata.zhaoping.handlemr.jar.writable;

import cn.adam.bigdata.zhaoping.entity.CSVFormats;
import cn.adam.bigdata.zhaoping.util.Utils;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

@Getter
@Setter
public class JobWritable implements WritableComparable<JobWritable> {
    private String company_financing_stage;
    private String company_industry;
    private String company_location;
    private String company_name;
    private String company_nature;
    private String company_overview;
    private String company_people;
    private String company_people_handle;
    private String company_people_level;
    private String job_edu_require;
    private String job_edu_require_handle;
    private String job_edu_require_level;
    private String job_exp_require;
    private String job_exp_require_handle;
    private String job_exp_require_level;
    private String job_info;
    private String job_info_words;
    private String job_name;
    private String job_salary;
    private String job_salary_handle;
    private String job_salary_level;
    private String job_tag;
    private String job_welfare;
    private String job_welfare_words;

    public static JobWritable parse(Map<String, String> map){
        JobWritable jobWritable = new JobWritable();
        Utils.mapToObject(map, jobWritable);
        return jobWritable;
    }

    @Override
    public int compareTo(JobWritable o) {
        String ts = this.getText();
        String os = o.getText();
        return ts.compareTo(os);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Utils.writeDataOutput(dataOutput, this);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        Utils.readDataInput(dataInput, this);
    }

    @Override
    public String toString() {
        return Utils.objectToCsvstr(this, CSVFormats.AFTER).split("\n")[1];
    }

    public String getText() {
        return this.company_location
                +this.company_name
                +this.job_info
                +this.job_name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobWritable that = (JobWritable) o;
        return getText().equals(that.getText());
    }

    @Override
    public int hashCode() {
        return getText().hashCode();
    }
}
