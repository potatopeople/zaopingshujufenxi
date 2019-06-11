package cn.adam.bigdata.zhaoping.analyzedata.entity;

import java.util.Objects;

public class CountValue implements Comparable<CountValue> {
    private String name;
    private Integer count;

    public CountValue() {}

    public CountValue(String name, Integer count) {
        this.name = name;
        this.count = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CountValue that = (CountValue) o;
        return Objects.equals(getName(), that.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName());
    }

    @Override
    public String toString() {
        return "CountValue{" +
                "name='" + name + '\'' +
                ", count=" + count +
                '}';
    }

    @Override
    public int compareTo(CountValue o) {
        return o.count.compareTo(this.count);
    }
}
