package cn.adam.bigdata.zhaoping.analyzedata.entity;

import java.util.Objects;

public class XYValue implements Comparable<XYValue> {
    private Integer x;
    private Integer count;

    public XYValue() {}

    public XYValue(Integer x, Integer count) {
        this.x = x;
        this.count = count;
    }

    @Override
    public String toString() {
        return "XYValue{" +
                "x=" + x +
                ", count=" + count +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XYValue xyValue = (XYValue) o;
        return Objects.equals(x, xyValue.x) &&
                Objects.equals(count, xyValue.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, count);
    }

    public Integer getX() {
        return x;
    }

    public void setX(Integer x) {
        this.x = x;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public int compareTo(XYValue o) {
        return o.count.compareTo(this.count);
    }
}
