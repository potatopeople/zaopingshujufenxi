package cn.adam.bigdata.zhaoping.analyzedata.entity;

import java.util.Objects;

public class XYVValue {
    private Integer exp;
    private Integer edu;
    private Integer salary;

    public XYVValue() {
    }

    public XYVValue(Integer exp, Integer edu, Integer salary) {
        this.exp = exp;
        this.edu = edu;
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "XYVValue{" +
                "exp=" + exp +
                ", edu='" + edu + '\'' +
                ", salary=" + salary +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XYVValue xyvValue = (XYVValue) o;
        return Objects.equals(getExp(), xyvValue.getExp()) &&
                Objects.equals(getEdu(), xyvValue.getEdu()) &&
                Objects.equals(getSalary(), xyvValue.getSalary());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getExp(), getEdu(), getSalary());
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
}
