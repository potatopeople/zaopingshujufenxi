package cn.adam.bigdata.zhaoping.analyzedata.entity;

import java.util.Objects;

public class XYZVValue {
    private Integer exp;
    private Integer edu;
    private Integer salary;
    private String jobtag;

    public XYZVValue() {
    }

    public XYZVValue(Integer exp, Integer edu, Integer salary, String jobtag) {
        this.exp = exp;
        this.edu = edu;
        this.salary = salary;
        this.jobtag = jobtag;
    }

    @Override
    public String toString() {
        return "XYZVValue{" +
                "exp=" + exp +
                ", edu='" + edu + '\'' +
                ", salary=" + salary +
                ", jobtag='" + jobtag + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XYZVValue xyzvValue = (XYZVValue) o;
        return Objects.equals(exp, xyzvValue.exp) &&
                Objects.equals(edu, xyzvValue.edu) &&
                Objects.equals(salary, xyzvValue.salary) &&
                Objects.equals(jobtag, xyzvValue.jobtag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exp, edu, salary, jobtag);
    }

    public Integer getExp() {
        return exp;
    }

    public void setExp(Integer exp) {
        this.exp = exp;
    }

    public Integer getEdu() {
        return edu;
    }

    public void setEdu(Integer edu) {
        this.edu = edu;
    }

    public Integer getSalary() {
        return salary;
    }

    public void setSalary(Integer salary) {
        this.salary = salary;
    }

    public String getJobtag() {
        return jobtag;
    }

    public void setJobtag(String jobtag) {
        this.jobtag = jobtag;
    }
}
